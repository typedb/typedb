/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::Instant,
};

use database::{
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionSchema, TransactionWrite},
    Database,
};
use durability::wal::{print_wal_instrumentation, reset_wal_instrumentation};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use storage::{
    durability_client::WALClient,
    isolation_manager::{print_isolation_instrumentation, reset_isolation_instrumentation},
    print_commit_instrumentation, reset_commit_instrumentation,
};
use test_utils::{create_tmp_dir, TempDir};

const INSERTS_PER_TRANSACTION: usize = 100;
const TOTAL_INSERTS: usize = 100_000;

const DB_NAME: &str = "bench-concurrent-writes";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name, owns age;
"#;

struct PhaseTimings {
    open_nanos: AtomicU64,
    execute_nanos: AtomicU64,
    commit_nanos: AtomicU64,
    tx_count: AtomicU64,
}

impl PhaseTimings {
    fn new() -> Self {
        Self {
            open_nanos: AtomicU64::new(0),
            execute_nanos: AtomicU64::new(0),
            commit_nanos: AtomicU64::new(0),
            tx_count: AtomicU64::new(0),
        }
    }

    fn record(&self, open_ns: u64, execute_ns: u64, commit_ns: u64) {
        self.open_nanos.fetch_add(open_ns, Ordering::Relaxed);
        self.execute_nanos.fetch_add(execute_ns, Ordering::Relaxed);
        self.commit_nanos.fetch_add(commit_ns, Ordering::Relaxed);
        self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> (f64, f64, f64, u64) {
        let count = self.tx_count.load(Ordering::Relaxed);
        if count == 0 {
            return (0.0, 0.0, 0.0, 0);
        }
        let open_avg = self.open_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        let exec_avg = self.execute_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        let commit_avg = self.commit_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        (open_avg, exec_avg, commit_avg, count)
    }
}

fn setup() -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    tx.commit().1.unwrap();

    (tmp_dir, database)
}

fn execute_batch(database: &Arc<Database<WALClient>>, batch_id: usize, timings: &PhaseTimings) {
    let t0 = Instant::now();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    for i in 0..INSERTS_PER_TRANSACTION {
        let age: u32 = rand::random();
        let name_id = batch_id * INSERTS_PER_TRANSACTION + i;
        let query_str = format!(r#"insert $p isa person, has name "person_{name_id}", has age {age};"#);
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            pipeline,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record(
        (t1 - t0).as_nanos() as u64,
        (t2 - t1).as_nanos() as u64,
        (t3 - t2).as_nanos() as u64,
    );
}

fn run_benchmark(num_threads: usize) {
    let total_transactions = TOTAL_INSERTS / INSERTS_PER_TRANSACTION;
    let transactions_per_thread = total_transactions / num_threads;

    let (_tmp_dir, database) = setup();
    let timings = Arc::new(PhaseTimings::new());

    // Reset all instrumentation counters for this run
    reset_wal_instrumentation();
    reset_isolation_instrumentation();
    reset_commit_instrumentation();

    let start_signal = Arc::new(RwLock::new(()));
    let write_guard = start_signal.write().unwrap();

    let join_handles: Vec<JoinHandle<()>> = (0..num_threads)
        .map(|thread_id| {
            let db = database.clone();
            let signal = start_signal.clone();
            let timings = timings.clone();
            thread::spawn(move || {
                drop(signal.read().unwrap());
                for batch in 0..transactions_per_thread {
                    let batch_id = thread_id * transactions_per_thread + batch;
                    execute_batch(&db, batch_id, &timings);
                }
            })
        })
        .collect();

    let start = Instant::now();
    drop(write_guard);

    for handle in join_handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_inserts = num_threads * transactions_per_thread * INSERTS_PER_TRANSACTION;
    let total_txns = num_threads * transactions_per_thread;
    let inserts_per_sec = total_inserts as f64 / elapsed.as_secs_f64();
    let txns_per_sec = total_txns as f64 / elapsed.as_secs_f64();

    let (open_us, exec_us, commit_us, count) = timings.summary();
    let total_us = open_us + exec_us + commit_us;

    let label = format!("{num_threads:>3}T");
    eprintln!(
        "Threads: {:>3} | Time: {:>8.1}ms | Inserts/s: {:>10.0} | Txns/s: {:>8.0} | \
         avg tx: open {:>8.0}us ({:>4.1}%) exec {:>8.0}us ({:>4.1}%) commit {:>8.0}us ({:>4.1}%) [{} txns]",
        num_threads,
        elapsed.as_secs_f64() * 1000.0,
        inserts_per_sec,
        txns_per_sec,
        open_us,
        open_us / total_us * 100.0,
        exec_us,
        exec_us / total_us * 100.0,
        commit_us,
        commit_us / total_us * 100.0,
        count,
    );
    print_wal_instrumentation(&label);
    print_isolation_instrumentation(&label);
    print_commit_instrumentation(&label);
    eprintln!();
}

fn main() {
    let thread_counts = [1, 2, 4, 8, 16, 32, 64, 128];
    eprintln!("Concurrent Write Scalability Benchmark");
    eprintln!("======================================");
    eprintln!("Inserts per transaction: {INSERTS_PER_TRANSACTION}");
    eprintln!("Total inserts per run:   {TOTAL_INSERTS}");
    eprintln!();

    for &num_threads in &thread_counts {
        run_benchmark(num_threads);
    }
}
