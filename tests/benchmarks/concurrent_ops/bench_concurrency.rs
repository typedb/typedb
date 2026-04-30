/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    env,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    thread::JoinHandle,
    time::Instant,
};

use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, TransactionRead, TransactionSchema, TransactionWrite},
};
use executor::{ExecutionInterrupt, pipeline::stage::StageIterator};
use options::{QueryOptions, TransactionOptions};
use rand_core::RngCore;
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_storage_dir};
use xoshiro::Xoshiro256Plus;

const TOTAL_OPS: usize = 300_000;
const READ_OPS: usize = 100_000;

const DB_NAME: &str = "bench-concurrency";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    attribute score value double;
    entity person owns name, owns age, owns score @card(0..);
    relation friendship relates friend @card(0..);
    person plays friendship:friend;
"#;

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
    let tmp_dir = create_tmp_storage_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();

    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    (tmp_dir, database)
}

fn seed_persons(database: &Arc<Database<WALClient>>, count: usize) {
    let batch_size = 1000;
    let mut offset = 0;
    while offset < count {
        let n = std::cmp::min(batch_size, count - offset);
        let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        for i in 0..n {
            let id = offset + i;
            let age: u32 = (id % 100) as u32;
            let query_str = format!(r#"insert $p isa person, has name "person_{id}", has age {age};"#);
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
        let (mut profile, intent) = tx.finalise();
        intent.unwrap().commit(profile.commit_profile()).unwrap();
        offset += n;
    }
}

// --- Write transaction helpers ---

fn execute_insert_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    let mut rng = Xoshiro256Plus::from_seed_u64(rand::random());
    for i in 0..ops_per_tx {
        let age: u32 = rng.next_u64() as u32;
        let name_id = batch_id * ops_per_tx + i;
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

    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
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
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    let mut rng = Xoshiro256Plus::from_seed_u64(rand::random());
    for i in 0..ops_per_tx {
        let person_id = (batch_id * ops_per_tx + i) % seed_count;
        let score: f64 = rng.next_u64() as u32 as f64 / 100.0;
        let query_str = format!(r#"match $p isa person, has name "person_{person_id}"; insert $p has score {score};"#);
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

    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
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
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    for i in 0..ops_per_tx {
        let idx = batch_id * ops_per_tx + i;
        let a_id = idx % seed_count;
        let b_id = (idx + 1) % seed_count;
        let query_str = format!(
            r#"match $a isa person, has name "person_{a_id}"; $b isa person, has name "person_{b_id}"; insert friendship (friend: $a, friend: $b);"#
        );
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

    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
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
            &query,
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
    print_header("PureInsert", batch_size);
    for &num_threads in thread_counts {
        let (_tmp_dir, database) = create_database(SCHEMA);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = TOTAL_OPS / batch_size;
        let actual_ops = total_transactions * batch_size;

        let elapsed =
            run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, |db, batch_id, ops, t| {
                execute_insert_batch(db, batch_id, ops, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings, show_dist);
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
        let total_transactions = TOTAL_OPS / batch_size;
        let actual_ops = total_transactions * batch_size;

        let seed_count = UPDATE_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, move |db, batch_id, ops, t| {
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
        let total_transactions = TOTAL_OPS / batch_size;
        let actual_ops = total_transactions * batch_size;

        let seed_count = RELATION_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, move |db, batch_id, ops, t| {
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
        let total_write_txns = TOTAL_OPS / ops_per_write_tx;
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

        let ops_per_thread = READ_OPS / num_threads;
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

fn main() {
    let thread_counts = [1, 4, 8];
    let show_dist = env::var("BENCH_DIST").is_ok();

    eprintln!("Concurrent Write Scalability Benchmark Suite");
    eprintln!("=============================================");
    eprintln!("Total ops per write workload: {TOTAL_OPS}");
    eprintln!("Total ops for pure read:      {READ_OPS}");
    if show_dist {
        eprintln!("Distribution output:          enabled (BENCH_DIST)");
    }
    eprintln!();

    // W1: Pure Insert
    for &batch_size in &[1000, 100, 1] {
        run_pure_insert_benchmark(&thread_counts, batch_size, show_dist);
    }

    // W2: Pure Update (match-insert generating Puts)
    for &batch_size in &[1000, 100, 1] {
        run_pure_update_benchmark(&thread_counts, batch_size, show_dist);
    }

    // W3: Insert Relations
    for &batch_size in &[1000, 100, 1] {
        run_insert_relation_benchmark(&thread_counts, batch_size, show_dist);
    }

    // W4: Mixed 50/50
    for &batch_size in &[1000, 100, 1] {
        run_mixed_benchmark(&thread_counts, batch_size, 0.5, show_dist);
    }

    // W5: Mixed 20/80
    for &batch_size in &[1000, 100, 1] {
        run_mixed_benchmark(&thread_counts, batch_size, 0.2, show_dist);
    }

    // W6: Pure Read
    run_pure_read_benchmark(&thread_counts);
}
