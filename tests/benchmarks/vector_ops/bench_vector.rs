/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! YCSB-style vector search micro-benchmarks: write, read, mixed.
//! Each workload reports per-op latency (p50/p99) and aggregate throughput across thread counts.
//! Ops go through TypeQL + transactions, i.e. the same path a client query takes.

#![deny(unused_must_use)]

use std::{
    cell::Cell,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, TransactionRead, TransactionSchema, TransactionWrite},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use executor::{ExecutionInterrupt, pipeline::stage::StageIterator};
use options::{MvccCleanupStrategy, QueryOptions, TransactionOptions, byte_size::ByteSize};
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_storage_dir};

const DIMENSIONS: usize = 128;
const SEED_VECTORS: usize = 10_000;
const WRITE_OPS: usize = 2_000;
const READ_OPS: usize = 2_000;
const MIXED_OPS: usize = 2_000;
const THREAD_COUNTS: [usize; 3] = [1, 4, 8];
// Elements are uniform in [0, 1), so random pairs have cosine similarity ~0.75. Lower thresholds
// match more of the dataset and push the executor's k-widening loop harder.
const THRESHOLDS: [f64; 3] = [0.85, 0.8, 0.75];

type Db = Arc<Database<WALClient>>;

fn create_database() -> (TempDir, Db) {
    let tmp_dir = create_tmp_storage_dir();
    let dbm = DatabaseManager::new(
        &tmp_dir,
        Arc::new(DiagnosticsManager::new_disabled()),
        ByteSize::mb(64),
        ByteSize::mb(64),
        ImportOwnership::Exclusive,
        MvccCleanupStrategy::Disabled,
    )
    .unwrap();
    dbm.put_database("bench-vector").unwrap();
    let database = dbm.database("bench-vector").unwrap();

    let schema = format!(
        r#"define
            attribute embedding, value vector({DIMENSIONS}, "float32") @index(hnsw:cosine);
            attribute tag, value string;
            entity item owns embedding @card(0..), owns tag @card(0..);"#
    );
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) =
        execute_schema_query(tx, typeql::parse_query(&schema).unwrap().into_structure().into_schema(), schema.clone());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    // every seeded item has a vector and a "tag-<n>" string, so both kinds of read have data
    let next_tag = Cell::new(0);
    let seed_attributes = || {
        next_tag.set(next_tag.get() + 1);
        format!(r#"{}, has tag "tag-{}""#, vector_attribute(), next_tag.get() - 1)
    };
    for _ in 0..SEED_VECTORS / 1000 {
        insert(&database, 1000, seed_attributes);
    }
    (tmp_dir, database)
}

fn vector_attribute() -> String {
    format!("has embedding {}", random_vector_literal())
}

/// Baseline: a unique string attribute, to compare vector writes against.
fn string_attribute() -> String {
    format!(r#"has tag "tag-{}""#, rand::random::<u64>())
}

fn random_vector_literal() -> String {
    let elements: Vec<String> = (0..DIMENSIONS).map(|_| format!("{:?}", rand::random::<f32>())).collect();
    format!(r#"vector([{}], "float32")"#, elements.join(", "))
}

/// One write transaction inserting `count` items, one query each, each owning one `attribute()`.
fn insert(database: &Db, count: usize, attribute: impl Fn() -> String) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    for _ in 0..count {
        let query = format!("insert $i isa item, {};", attribute());
        let pipeline = typeql::parse_query(&query).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            Arc::new(pipeline),
            None::<GivenRowsSimple>,
            query,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

/// One read transaction running one similarity search; returns the number of answers.
fn search(database: &Db, threshold: f64) -> usize {
    let query =
        format!("match let $e, $s in cosine_similarity_search(embedding, {}, {threshold:?});", random_vector_literal());
    read(database, &query)
}

/// Vector read for the vector-vs-string comparisons: the threshold that returns a handful of answers.
fn search_selective(database: &Db) -> usize {
    search(database, THRESHOLDS[0])
}

/// Baseline: one read transaction looking up a seeded string attribute by value (1 answer).
fn lookup_string(database: &Db) -> usize {
    let query = format!(r#"match $t isa tag; $t == "tag-{}";"#, rand::random::<u64>() % SEED_VECTORS as u64);
    read(database, &query)
}

fn read(database: &Db, query: &str) -> usize {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let parsed = typeql::parse_query(&query).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone(),
            type_manager,
            thing_manager.clone(),
            function_manager.clone(),
            &parsed,
            None::<GivenRowsSimple>,
            query,
            None,
        )
        .unwrap();
    let (rows, _context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    rows.collect_owned().unwrap().len()
}

/// Runs `op` `ops_per_thread` times on each of `threads` threads; returns wall time and per-op latencies.
fn run_parallel(threads: usize, ops_per_thread: usize, op: impl Fn() + Sync) -> (Duration, Vec<Duration>) {
    let start = Instant::now();
    let latencies = thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| scope.spawn(|| (0..ops_per_thread).map(|_| timed(&op)).collect::<Vec<_>>()))
            .collect();
        handles.into_iter().flat_map(|handle| handle.join().unwrap()).collect()
    });
    (start.elapsed(), latencies)
}

fn timed(op: impl Fn()) -> Duration {
    let start = Instant::now();
    op();
    start.elapsed()
}

/// Latencies are per call; throughput counts `ops_per_call` ops per call.
fn report(label: &str, threads: usize, elapsed: Duration, ops_per_call: usize, mut latencies: Vec<Duration>) {
    latencies.sort_unstable();
    let calls = latencies.len();
    let n = calls * ops_per_call;
    let percentile = |p: f64| latencies[((calls as f64 * p) as usize).min(calls - 1)].as_secs_f64() * 1e6;
    eprintln!(
        "{label:<28} threads {threads:>2} | ops {n:>6} | ops/s {:>9.0} | p50 {:>9.0}us | p99 {:>9.0}us",
        n as f64 / elapsed.as_secs_f64(),
        percentile(0.50),
        percentile(0.99),
    );
}

fn bench_write() {
    let attributes: [(&str, fn() -> String); 2] = [("vector", vector_attribute), ("string", string_attribute)];
    for batch in [1, 100] {
        eprintln!("\n=== write: {batch} insert(s) per transaction, p50/p99 per transaction ===");
        for (name, attribute) in attributes {
            for threads in THREAD_COUNTS {
                let (_tmp, database) = create_database();
                let (elapsed, latencies) =
                    run_parallel(threads, WRITE_OPS / batch / threads, || insert(&database, batch, attribute));
                report(&format!("write {name}"), threads, elapsed, batch, latencies);
            }
        }
    }
}

fn bench_read(database: &Db) {
    eprintln!("\n=== read: 1 search per transaction, {SEED_VECTORS} vectors ===");
    for threshold in THRESHOLDS {
        eprintln!("threshold {threshold}: ~{} answers per search", search(database, threshold));
        for threads in THREAD_COUNTS {
            let (elapsed, latencies) = run_parallel(threads, READ_OPS / threads, || {
                search(database, threshold);
            });
            report(&format!("read vector @{threshold}"), threads, elapsed, 1, latencies);
        }
    }
    eprintln!("string lookup: ~{} answers per read", lookup_string(database));
    for threads in THREAD_COUNTS {
        let (elapsed, latencies) = run_parallel(threads, READ_OPS / threads, || {
            lookup_string(database);
        });
        report("read string", threads, elapsed, 1, latencies);
    }
}

/// YCSB-style: every thread picks each op at random, 50% insert (1 per transaction) / 50% read.
fn bench_mixed() {
    eprintln!("\n=== mixed: each op 50% insert / 50% read; vector reads @{} ===", THRESHOLDS[0]);
    let kinds: [(&str, fn() -> String, fn(&Db) -> usize); 2] =
        [("vector", vector_attribute, search_selective), ("string", string_attribute, lookup_string)];
    for (name, attribute, read) in kinds {
        for threads in THREAD_COUNTS {
            let (_tmp, database) = create_database();
            let start = Instant::now();
            let (writes, reads): (Vec<_>, Vec<_>) = thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|_| {
                        scope.spawn(|| {
                            (0..MIXED_OPS / threads)
                                .map(|_| {
                                    let write = rand::random::<bool>();
                                    let latency = timed(|| match write {
                                        true => insert(&database, 1, attribute),
                                        false => drop(read(&database)),
                                    });
                                    (write, latency)
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();
                handles.into_iter().flat_map(|handle| handle.join().unwrap()).partition(|(write, _)| *write)
            });
            let elapsed = start.elapsed();
            let latencies = |ops: Vec<(bool, Duration)>| ops.into_iter().map(|(_, latency)| latency).collect();
            let all: Vec<_> = writes.iter().chain(reads.iter()).map(|(_, latency)| *latency).collect();
            report(&format!("mixed {name} all"), threads, elapsed, 1, all);
            report(&format!("mixed {name} write"), threads, elapsed, 1, latencies(writes));
            report(&format!("mixed {name} read"), threads, elapsed, 1, latencies(reads));
        }
    }
}

fn main() {
    let workload = std::env::args().nth(1).unwrap_or_default();
    eprintln!("Vector benchmark: {DIMENSIONS} dims, {SEED_VECTORS} seed vectors");
    if workload.is_empty() || workload == "write" {
        bench_write();
    }
    if workload.is_empty() || workload == "read" {
        // ponytail: one shared dataset for all read runs; reads don't mutate it
        let (_tmp, database) = create_database();
        bench_read(&database);
    }
    if workload.is_empty() || workload == "mixed" {
        bench_mixed();
    }
}
