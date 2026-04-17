/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    env,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::Instant,
};

use database::{
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::{pipeline::stage::StageIterator, ExecutionInterrupt};
use options::{QueryOptions, TransactionOptions};
use query::query_manager::PipelinePayload;
use rand_core::RngCore;
use storage::{durability_client::WALClient, COMMIT_PHASE_STATS, WAL_WRITE_PHASE_STATS};
use test_utils::{create_tmp_dir, TempDir};
use xoshiro::Xoshiro256Plus;

// Total operations per write workload. Can be overridden at runtime with
// BENCH_TOTAL_OPS=N env var for quicker smoke runs.
const DEFAULT_TOTAL_OPS: usize = 300_000;
const DEFAULT_READ_OPS: usize = 100_000;

fn total_ops() -> usize {
    env::var("BENCH_TOTAL_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_TOTAL_OPS)
}

fn read_ops() -> usize {
    env::var("BENCH_READ_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_READ_OPS)
}

const DB_NAME: &str = "bench-concurrency";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    attribute score value double;
    entity person owns name, owns age, owns score @card(0..);
    relation friendship relates friend @card(0..);
    person plays friendship:friend;
"#;

// Inputs-stage query strings: each is compiled once per transaction and executed
// with N input rows, amortizing parse + compilation over the whole batch. The
// typed inputs declaration prepends a `inputs $v: T;` stage that is evaluated
// row-by-row against the pipeline body. Parenthesising input value references in
// `has` clauses (e.g. `has name ($n)`) forces the parser to treat them as
// expressions rather than attribute-variable aliases, avoiding a category
// conflict with the Value category assigned by the inputs stage.
const INSERT_QUERY: &str =
    r#"inputs $n: string, $a: integer; insert $p isa person, has name == $n, has age == $a;"#;

const UPDATE_QUERY: &str =
    r#"inputs $n: string, $s: double; match $p isa person, has name == $n; insert $p has score == $s;"#;

const RELATION_QUERY: &str =
    r#"inputs $an: string, $bn: string; match $a isa person, has name == $an; $b isa person, has name == $bn; insert friendship (friend: $a, friend: $b);"#;

fn quoted_string_literal(value: &str) -> String {
    // Inputs rows are parsed server-side via `typeql::parse_value(&str)`. String
    // values therefore need their quotes preserved; the schema-generated names
    // here never contain quotes or backslashes so we skip escaping.
    format!(r#""{value}""#)
}

fn make_payload(query: &str, inputs: Vec<Vec<Option<String>>>) -> PipelinePayload {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    PipelinePayload { parsed, inputs: Some(inputs) }
}

struct TxSample {
    open_ns: u64,
    exec_ns: u64,
    commit_ns: u64,
}

struct PhaseTimings {
    samples: Mutex<Vec<TxSample>>,
}

impl PhaseTimings {
    fn new() -> Self {
        Self { samples: Mutex::new(Vec::new()) }
    }

    fn record(&self, open_ns: u64, exec_ns: u64, commit_ns: u64) {
        self.samples.lock().unwrap().push(TxSample { open_ns, exec_ns, commit_ns });
    }

    fn analyze(&self) -> TimingAnalysis {
        let samples = self.samples.lock().unwrap();
        let count = samples.len();
        if count == 0 {
            return TimingAnalysis::empty();
        }

        let mut open: Vec<u64> = samples.iter().map(|s| s.open_ns).collect();
        let mut exec: Vec<u64> = samples.iter().map(|s| s.exec_ns).collect();
        let mut commit: Vec<u64> = samples.iter().map(|s| s.commit_ns).collect();
        let mut total: Vec<u64> = samples.iter().map(|s| s.open_ns + s.exec_ns + s.commit_ns).collect();
        open.sort_unstable();
        exec.sort_unstable();
        commit.sort_unstable();
        total.sort_unstable();

        TimingAnalysis {
            count,
            open: DistStats::from_sorted(&open),
            exec: DistStats::from_sorted(&exec),
            commit: DistStats::from_sorted(&commit),
            total: DistStats::from_sorted(&total),
        }
    }
}

#[derive(Clone, Copy)]
struct DistStats {
    mean_us: f64,
    min_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

impl DistStats {
    fn from_sorted(sorted: &[u64]) -> Self {
        let n = sorted.len();
        let sum: u64 = sorted.iter().sum();
        Self {
            mean_us: sum as f64 / n as f64 / 1000.0,
            min_us: sorted[0] as f64 / 1000.0,
            p50_us: sorted[n / 2] as f64 / 1000.0,
            p95_us: sorted[(n as f64 * 0.95) as usize] as f64 / 1000.0,
            p99_us: sorted[(n as f64 * 0.99) as usize] as f64 / 1000.0,
            max_us: sorted[n - 1] as f64 / 1000.0,
        }
    }
}

struct TimingAnalysis {
    count: usize,
    open: DistStats,
    exec: DistStats,
    commit: DistStats,
    total: DistStats,
}

impl TimingAnalysis {
    fn empty() -> Self {
        let zero = DistStats { mean_us: 0.0, min_us: 0.0, p50_us: 0.0, p95_us: 0.0, p99_us: 0.0, max_us: 0.0 };
        Self {
            count: 0,
            open: zero,
            exec: DistStats { mean_us: 0.0, min_us: 0.0, p50_us: 0.0, p95_us: 0.0, p99_us: 0.0, max_us: 0.0 },
            commit: DistStats { mean_us: 0.0, min_us: 0.0, p50_us: 0.0, p95_us: 0.0, p99_us: 0.0, max_us: 0.0 },
            total: zero,
        }
    }
}

// --- Database setup helpers ---

fn create_database(schema: &str) -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();

    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    tx.commit().1.unwrap();

    (tmp_dir, database)
}

fn seed_persons(database: &Arc<Database<WALClient>>, count: usize) {
    let batch_size = 1000;
    let mut offset = 0;
    while offset < count {
        let n = std::cmp::min(batch_size, count - offset);
        let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        let inputs: Vec<Vec<Option<String>>> = (0..n)
            .map(|i| {
                let id = offset + i;
                let age: u32 = (id % 100) as u32;
                vec![Some(quoted_string_literal(&format!("person_{id}"))), Some(age.to_string())]
            })
            .collect();
        let payload = make_payload(INSERT_QUERY, inputs);
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            payload,
            INSERT_QUERY.to_string(),
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
        tx.commit().1.unwrap();
        offset += n;
    }
}

// --- Write transaction helpers ---
//
// Each helper opens one write transaction, submits a single inputs-stage query
// containing `ops_per_tx` input rows, commits, and records the open / exec /
// commit timing split. Bulk loading therefore pays parse + compile costs once
// per batch instead of once per row, which is the hot path krishnan's
// add-inputs-stage patch was designed to accelerate.

fn execute_insert_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    let mut rng = Xoshiro256Plus::from_seed_u64(rand::random());
    let inputs: Vec<Vec<Option<String>>> = (0..ops_per_tx)
        .map(|i| {
            let age: u32 = rng.next_u64() as u32;
            let name_id = batch_id * ops_per_tx + i;
            vec![Some(quoted_string_literal(&format!("person_{name_id}"))), Some(age.to_string())]
        })
        .collect();
    let payload = make_payload(INSERT_QUERY, inputs);
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        payload,
        INSERT_QUERY.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap();
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record((t1 - t0).as_nanos() as u64, (t2 - t1).as_nanos() as u64, (t3 - t2).as_nanos() as u64);
}

fn execute_update_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    seed_count: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    let mut rng = Xoshiro256Plus::from_seed_u64(rand::random());
    let inputs: Vec<Vec<Option<String>>> = (0..ops_per_tx)
        .map(|i| {
            let person_id = (batch_id * ops_per_tx + i) % seed_count;
            let score: f64 = rng.next_u64() as u32 as f64 / 100.0;
            // Force a decimal point so the literal is parsed as double, not integer.
            vec![Some(quoted_string_literal(&format!("person_{person_id}"))), Some(format!("{score:.6}"))]
        })
        .collect();
    let payload = make_payload(UPDATE_QUERY, inputs);
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        payload,
        UPDATE_QUERY.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap();
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record((t1 - t0).as_nanos() as u64, (t2 - t1).as_nanos() as u64, (t3 - t2).as_nanos() as u64);
}

fn execute_relation_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    seed_count: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    let inputs: Vec<Vec<Option<String>>> = (0..ops_per_tx)
        .map(|i| {
            let idx = batch_id * ops_per_tx + i;
            let a_id = idx % seed_count;
            let b_id = (idx + 1) % seed_count;
            vec![
                Some(quoted_string_literal(&format!("person_{a_id}"))),
                Some(quoted_string_literal(&format!("person_{b_id}"))),
            ]
        })
        .collect();
    let payload = make_payload(RELATION_QUERY, inputs);
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        payload,
        RELATION_QUERY.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap();
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record((t1 - t0).as_nanos() as u64, (t2 - t1).as_nanos() as u64, (t3 - t2).as_nanos() as u64);
}

// --- Read transaction helper ---

fn execute_read_query(database: &Arc<Database<WALClient>>, query_str: &str) {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone(),
            type_manager,
            thing_manager.clone(),
            function_manager,
            PipelinePayload::from(query),
            query_str,
        )
        .unwrap();
    let (rows, _context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _batch = rows.collect_owned().unwrap();
}

// --- Reporting helpers ---

fn print_header(name: &str, batch_size: usize) {
    eprintln!();
    eprintln!("=== {name} | batch={batch_size} ===");
}

fn print_dist(label: &str, d: &DistStats) {
    eprintln!(
        "    {:<8} mean {:>10.0}us | p50 {:>10.0}us | p95 {:>10.0}us | p99 {:>10.0}us | max {:>10.0}us",
        label, d.mean_us, d.p50_us, d.p95_us, d.p99_us, d.max_us,
    );
}

fn print_result(
    num_threads: usize,
    elapsed: std::time::Duration,
    total_ops: usize,
    timings: &PhaseTimings,
    show_dist: bool,
) {
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let a = timings.analyze();
    eprintln!(
        "Threads: {:>3} | Time: {:>8.1}ms | Ops/s: {:>10.0} | [{} txns]",
        num_threads,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec,
        a.count,
    );
    if show_dist && a.count > 0 {
        print_dist("open", &a.open);
        print_dist("exec", &a.exec);
        print_dist("commit", &a.commit);
        print_dist("TOTAL", &a.total);
    }
}

fn print_mixed_result(
    num_threads: usize,
    write_threads: usize,
    read_threads: usize,
    elapsed: std::time::Duration,
    write_ops: usize,
    read_ops: usize,
    timings: &PhaseTimings,
    show_dist: bool,
) {
    let w_ops_s = write_ops as f64 / elapsed.as_secs_f64();
    let r_ops_s = read_ops as f64 / elapsed.as_secs_f64();
    let a = timings.analyze();
    eprintln!(
        "Threads: {:>3} ({:>2}W+{:>2}R) | Time: {:>8.1}ms | W-ops/s: {:>10.0} | R-ops/s: {:>10.0} | [{} w-txns]",
        num_threads,
        write_threads,
        read_threads,
        elapsed.as_secs_f64() * 1000.0,
        w_ops_s,
        r_ops_s,
        a.count,
    );
    if show_dist && a.count > 0 {
        print_dist("open", &a.open);
        print_dist("exec", &a.exec);
        print_dist("commit", &a.commit);
        print_dist("TOTAL", &a.total);
    }
}

// --- Concurrent runner ---

fn run_write_threads<F>(
    database: &Arc<Database<WALClient>>,
    num_threads: usize,
    ops_per_tx: usize,
    total_ops: usize,
    timings: &Arc<PhaseTimings>,
    thread_fn: F,
) -> std::time::Duration
where
    F: Fn(&Arc<Database<WALClient>>, usize, usize, &PhaseTimings) + Send + Sync + 'static,
{
    let total_transactions = total_ops / ops_per_tx;
    let next_batch = Arc::new(AtomicU64::new(0));
    let thread_fn = Arc::new(thread_fn);

    let start_signal = Arc::new(RwLock::new(()));
    let write_guard = start_signal.write().unwrap();

    let join_handles: Vec<JoinHandle<()>> = (0..num_threads)
        .map(|_thread_id| {
            let db = database.clone();
            let signal = start_signal.clone();
            let timings = timings.clone();
            let thread_fn = thread_fn.clone();
            let next_batch = next_batch.clone();
            let total = total_transactions;
            thread::spawn(move || {
                drop(signal.read().unwrap());
                loop {
                    let batch_id = next_batch.fetch_add(1, Ordering::Relaxed) as usize;
                    if batch_id >= total {
                        break;
                    }
                    thread_fn(&db, batch_id, ops_per_tx, &timings);
                }
            })
        })
        .collect();

    let start = Instant::now();
    drop(write_guard);

    for handle in join_handles {
        handle.join().unwrap();
    }

    start.elapsed()
}

// --- W1: Pure Insert ---

fn run_pure_insert_benchmark(thread_counts: &[usize], batch_size: usize, show_dist: bool) {
    let show_phases = env::var("BENCH_COMMIT_PHASES").is_ok();
    print_header("PureInsert", batch_size);
    for &num_threads in thread_counts {
        let (_tmp_dir, database) = create_database(SCHEMA);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = total_ops() / batch_size;
        let actual_ops = total_transactions * batch_size;

        if show_phases {
            COMMIT_PHASE_STATS.reset();
            WAL_WRITE_PHASE_STATS.reset();
        }

        let elapsed =
            run_write_threads(&database, num_threads, batch_size, total_ops(), &timings, |db, batch_id, ops, t| {
                execute_insert_batch(db, batch_id, ops, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings, show_dist);
        if show_phases {
            eprintln!("{}", COMMIT_PHASE_STATS.dump());
            eprintln!("{}", WAL_WRITE_PHASE_STATS.dump());
        }
    }
}

// --- W2: Pure Update (match-insert generating Puts) ---

const UPDATE_SEED_COUNT: usize = 10_000;

fn run_pure_update_benchmark(thread_counts: &[usize], batch_size: usize, show_dist: bool) {
    print_header("PureUpdate", batch_size);
    for &num_threads in thread_counts {
        if batch_size == 1 && num_threads > 16 {
            continue;
        }
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, UPDATE_SEED_COUNT);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = total_ops() / batch_size;
        let actual_ops = total_transactions * batch_size;

        let seed_count = UPDATE_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, total_ops(), &timings, move |db, batch_id, ops, t| {
                execute_update_batch(db, batch_id, ops, seed_count, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings, show_dist);
    }
}

// --- W3: Insert Relations ---

const RELATION_SEED_COUNT: usize = 10_000;

fn run_insert_relation_benchmark(thread_counts: &[usize], batch_size: usize, show_dist: bool) {
    print_header("InsertRelation", batch_size);
    for &num_threads in thread_counts {
        if batch_size == 1 && num_threads > 16 {
            continue;
        }
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, RELATION_SEED_COUNT);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = total_ops() / batch_size;
        let actual_ops = total_transactions * batch_size;

        let seed_count = RELATION_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, total_ops(), &timings, move |db, batch_id, ops, t| {
                execute_relation_batch(db, batch_id, ops, seed_count, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings, show_dist);
    }
}

// --- W4/W5: Mixed read/write ---

const MIXED_SEED_COUNT: usize = 10_000;

fn run_mixed_benchmark(thread_counts: &[usize], batch_size: usize, write_ratio: f64, show_dist: bool) {
    let pct = (write_ratio * 100.0) as usize;
    let name = format!("Mixed{pct}Write");
    print_header(&name, batch_size);

    for &num_threads in thread_counts {
        let write_threads = std::cmp::max(1, (num_threads as f64 * write_ratio).round() as usize);
        let read_threads = num_threads - write_threads;
        if read_threads == 0 {
            continue;
        }

        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, MIXED_SEED_COUNT);

        let write_timings = Arc::new(PhaseTimings::new());
        let write_ops_total = Arc::new(AtomicU64::new(0));
        let read_ops_total = Arc::new(AtomicU64::new(0));

        let ops_per_write_tx = batch_size;
        let total_write_txns = total_ops() / ops_per_write_tx;
        let next_write_batch = Arc::new(AtomicU64::new(0));

        let running = Arc::new(AtomicBool::new(true));

        let start_signal = Arc::new(RwLock::new(()));
        let write_guard = start_signal.write().unwrap();

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Spawn write threads
        for _thread_id in 0..write_threads {
            let db = database.clone();
            let signal = start_signal.clone();
            let timings = write_timings.clone();
            let ops_counter = write_ops_total.clone();
            let seed_count = MIXED_SEED_COUNT;
            let next_batch = next_write_batch.clone();
            let total = total_write_txns;
            handles.push(thread::spawn(move || {
                drop(signal.read().unwrap());
                loop {
                    let batch_id = next_batch.fetch_add(1, Ordering::Relaxed) as usize;
                    if batch_id >= total {
                        break;
                    }
                    execute_relation_batch(&db, batch_id, ops_per_write_tx, seed_count, &timings);
                    ops_counter.fetch_add(ops_per_write_tx as u64, Ordering::Relaxed);
                }
            }));
        }

        // Spawn read threads
        for _thread_id in 0..read_threads {
            let db = database.clone();
            let signal = start_signal.clone();
            let running = running.clone();
            let ops_counter = read_ops_total.clone();
            handles.push(thread::spawn(move || {
                drop(signal.read().unwrap());
                let mut count: u64 = 0;
                while running.load(Ordering::Relaxed) {
                    let age_threshold = (count % 100) as u32;
                    let query_str =
                        format!(r#"match $p isa person, has age > {age_threshold}, has name $n; limit 10;"#);
                    execute_read_query(&db, &query_str);
                    count += 1;
                }
                ops_counter.fetch_add(count, Ordering::Relaxed);
            }));
        }

        let start = Instant::now();
        drop(write_guard);

        // Wait for write threads, then signal readers to stop
        for handle in handles.drain(..write_threads) {
            handle.join().unwrap();
        }
        running.store(false, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();
        let w_ops = write_ops_total.load(Ordering::Relaxed) as usize;
        let r_ops = read_ops_total.load(Ordering::Relaxed) as usize;

        print_mixed_result(num_threads, write_threads, read_threads, elapsed, w_ops, r_ops, &write_timings, show_dist);
    }
}

// --- W6: Pure Read ---

const READ_SEED_COUNT: usize = 10_000;

fn run_pure_read_benchmark(thread_counts: &[usize]) {
    print_header("PureRead", 1);

    for &num_threads in thread_counts {
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, READ_SEED_COUNT);

        let ops_per_thread = read_ops() / num_threads;
        let actual_ops = ops_per_thread * num_threads;

        let start_signal = Arc::new(RwLock::new(()));
        let write_guard = start_signal.write().unwrap();

        let handles: Vec<JoinHandle<()>> = (0..num_threads)
            .map(|thread_id| {
                let db = database.clone();
                let signal = start_signal.clone();
                thread::spawn(move || {
                    drop(signal.read().unwrap());
                    for i in 0..ops_per_thread {
                        let age_threshold = ((thread_id * ops_per_thread + i) % 100) as u32;
                        let query_str =
                            format!(r#"match $p isa person, has age > {age_threshold}, has name $n; limit 10;"#);
                        execute_read_query(&db, &query_str);
                    }
                })
            })
            .collect();

        let start = Instant::now();
        drop(write_guard);

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();

        eprintln!(
            "Threads: {:>3} | Time: {:>8.1}ms | Reads/s: {:>10.0}",
            num_threads,
            elapsed.as_secs_f64() * 1000.0,
            actual_ops as f64 / elapsed.as_secs_f64(),
        );
    }
}

// --- Main ---

fn parse_csv_usize(var: &str, default: &[usize]) -> Vec<usize> {
    match env::var(var) {
        Ok(v) => v.split(',').filter_map(|s| s.trim().parse().ok()).collect(),
        Err(_) => default.to_vec(),
    }
}

fn parse_csv_str(var: &str, default: &[&str]) -> Vec<String> {
    match env::var(var) {
        Ok(v) => v.split(',').map(|s| s.trim().to_lowercase()).collect(),
        Err(_) => default.iter().map(|s| s.to_string()).collect(),
    }
}

fn main() {
    let thread_counts = parse_csv_usize("BENCH_THREADS", &[1, 4, 8]);
    let batch_sizes = parse_csv_usize("BENCH_BATCH_SIZES", &[1000, 100, 1]);
    let workloads = parse_csv_str(
        "BENCH_WORKLOADS",
        &["insert", "update", "relation", "mixed50", "mixed20", "read"],
    );
    let show_dist = env::var("BENCH_DIST").is_ok();

    eprintln!("Concurrent Write Scalability Benchmark Suite (inputs-stage bulk-load)");
    eprintln!("=====================================================================");
    eprintln!("Total ops per write workload: {}", total_ops());
    eprintln!("Total ops for pure read:      {}", read_ops());
    eprintln!("Threads:                      {:?}", thread_counts);
    eprintln!("Batch sizes:                  {:?}", batch_sizes);
    eprintln!("Workloads:                    {:?}", workloads);
    if show_dist {
        eprintln!("Distribution output:          enabled (BENCH_DIST)");
    }
    eprintln!();

    let run = |name: &str| workloads.iter().any(|w| w == name);

    if run("insert") {
        for &batch_size in &batch_sizes {
            run_pure_insert_benchmark(&thread_counts, batch_size, show_dist);
        }
    }
    if run("update") {
        for &batch_size in &batch_sizes {
            run_pure_update_benchmark(&thread_counts, batch_size, show_dist);
        }
    }
    if run("relation") {
        for &batch_size in &batch_sizes {
            run_insert_relation_benchmark(&thread_counts, batch_size, show_dist);
        }
    }
    if run("mixed50") {
        for &batch_size in &batch_sizes {
            run_mixed_benchmark(&thread_counts, batch_size, 0.5, show_dist);
        }
    }
    if run("mixed20") {
        for &batch_size in &batch_sizes {
            run_mixed_benchmark(&thread_counts, batch_size, 0.2, show_dist);
        }
    }
    if run("read") {
        run_pure_read_benchmark(&thread_counts);
    }
}
