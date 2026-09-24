/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use std::time::Instant;
use database::{Database, transaction::TransactionWrite};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

use crate::{
    Config, Context, QueryAnswer, commit,
    datagen::RandomDataGen,
    execute_write_query_in,
    profiling::TxQueryProfile,
    reports::SimpleReport,
    utils::{CountResults, unpack_result},
};
use crate::profiling::{transaction_options_with_profiling, MultiQueryTxProfile, MultiTxMultiQueryProfile};

/// See `BenchmarkRunner` implementations for the order in which these functions are called.
pub trait SimpleBenchmark {
    type RunInput;
    type IterOutput: SimpleReport;

    /// Create given_rows if needed
    fn name(&self) -> &'_ str;

    fn init_context(&self) -> Context {
        Context::init(Config::default())
    }

    /// Run before any iteration or batch.
    fn before_all(&self, _context: &mut Context) {}

    fn create_database(&self, context: &mut Context) -> Arc<Database<WALClient>> {
        context.recreate_database(self.name()).unwrap()
    }

    /// Load schema & data
    fn prepare_database(&self, context: &Context, database: Arc<Database<WALClient>>);

    fn warm_up(&self, context: &Context, database: Arc<Database<WALClient>>);
    /// Create given_rows if needed
    fn prepare_run(&self, context: &Context, database: Arc<Database<WALClient>>) -> Self::RunInput;

    /// The actual iteration which gets timed over and over again.
    fn run_benchmark(
        &self,
        context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::RunInput,
    ) -> Self::IterOutput;
}

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type WarmupFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareRunFn<IN> = Box<dyn Fn(Arc<Database<WALClient>>) -> IN>;
pub type BenchmarkedFn<IN, OUT> = Box<dyn Fn(Arc<Database<WALClient>>, IN) -> OUT>;

pub struct TypeDBMicroBenchmark<IN, OUT: SimpleReport> {
    pub name: &'static str,
    pub schema: &'static str,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub warmup_fn: Option<WarmupFn>,
    pub prepare_run_fn: PrepareRunFn<IN>,
    pub benchmark_fn: BenchmarkedFn<IN, OUT>,
}

impl<IN, OUT: SimpleReport> SimpleBenchmark for TypeDBMicroBenchmark<IN, OUT> {
    type RunInput = IN;
    type IterOutput = OUT;

    fn name(&self) -> &'_ str {
        self.name
    }

    fn prepare_database(&self, _context: &Context, database: Arc<Database<WALClient>>) {
        crate::create_schema(database.clone(), self.schema);
        if let Some(preload_fn) = &self.preload_data_fn {
            preload_fn(database.clone())
        }
    }

    fn warm_up(&self, _context: &Context, database: Arc<Database<WALClient>>) {
        if let Some(warmup_fn) = &self.warmup_fn {
            warmup_fn(database.clone())
        }
    }

    fn prepare_run(&self, _context: &Context, database: Arc<Database<WALClient>>) -> Self::RunInput {
        (self.prepare_run_fn)(database)
    }

    fn run_benchmark(
        &self,
        _context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::RunInput,
    ) -> Self::IterOutput {
        (self.benchmark_fn)(database, input)
    }
}

pub fn sanity_check() -> TypeDBMicroBenchmark<(), ()> {
    // Just to ensure the reported time is just the benchmark_fn
    TypeDBMicroBenchmark {
        name: "sanity_check",
        schema: "define entity person;",
        preload_data_fn: Some(Box::new(|_| std::thread::sleep(Duration::from_millis(40)))),
        warmup_fn: None,
        prepare_run_fn: Box::new(|_| std::thread::sleep(Duration::from_millis(20))),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_millis(10))),
    }
}

pub type TypeDBWorkloadBenchmark = TypeDBMicroBenchmark<Arc<WorkloadInstance>, MultiTxMultiQueryProfile>;

impl TypeDBWorkloadBenchmark {
    fn new(name: &'static str, schema: &'static str, preload_data_fn: Option<PreloadDataFn>, query_descriptor: QueryDescriptor, run_descriptor: RunDescriptor) -> Self {
        let warmup_fn = query_descriptor.for_warmup();
        let workload = WorkLoad { query_descriptor, run_descriptor };
        let prepare_run_fn = workload.prepare_fn();
        let benchmark_fn = workload.runner(name);
        Self {
            name,
            schema,
            preload_data_fn,
            warmup_fn,
            prepare_run_fn,
            benchmark_fn,
        }
    }
}

#[derive(Clone)]
pub struct QueryDescriptor {
    pub query: &'static str,
    pub variables: Vec<String>,
    pub produce_row: Option<fn(&mut RandomDataGen) -> Vec<GivenRowEntry>>,
    //Box<dyn Fn(&mut RandomDataGen) -> Option<GivenRowsSimple>>,
}

impl QueryDescriptor {

    pub fn for_warmup(&self) -> Option<WarmupFn> {
        let warmup_workload = self.create_workload(RunDescriptor::WARMUP.clone());
        let runner = warmup_workload.runner("_warmup");
        let prepare_fn = warmup_workload.prepare_fn();
        Some(Box::new(move |db| {
            let db_clone = db.clone();
            runner(db, prepare_fn(db_clone));
        }))
    }

    pub fn create_workload(&self, run_descriptor: RunDescriptor) -> WorkLoad {
        WorkLoad { query_descriptor: self.clone(), run_descriptor }
    }
}

#[derive(Clone)]
pub struct RunDescriptor {
    pub parallelism: usize,
    pub total_txns: usize,
    pub n_queries_per_tx: usize,
    pub n_rows_per_query: usize,
}

impl RunDescriptor {
    const WARMUP: RunDescriptor = RunDescriptor {
        parallelism: 1,
        total_txns: 10,
        n_queries_per_tx: 1,
        n_rows_per_query: 1,
    };

    pub fn total_rows(&self) -> usize {
        self.total_txns * self.n_queries_per_tx * self.n_rows_per_query
    }
}

// Util return
pub struct WorkLoad {
    pub query_descriptor: QueryDescriptor,
    pub run_descriptor: RunDescriptor,
}

impl WorkLoad {
    pub fn prepare_fn(&self) -> PrepareRunFn<Arc<WorkloadInstance>> {
        let query_descriptor = self.query_descriptor.clone();
        let run_descriptor = self.run_descriptor.clone();
        Box::new(move |_| WorkloadInstance::build(&query_descriptor, &run_descriptor))
    }

    pub fn runner(&self, name: &'static str) -> BenchmarkedFn<Arc<WorkloadInstance>, MultiTxMultiQueryProfile> {
        let query = self.query_descriptor.query;
        let run_descriptor = self.run_descriptor.clone();
        Box::new(move |database: Arc<Database<WALClient>>, producer: Arc<WorkloadInstance>| {
            let very_beginning = Instant::now();
            let handles: Vec<_> = (0..run_descriptor.parallelism)
                .map(|_thread_id| {
                    let database = database.clone();
                    let producer = producer.clone();
                    let run_descriptor = run_descriptor.clone();
                    std::thread::spawn(Self::new_runner_thread(database, producer, query, run_descriptor))
                })
                .collect();

            let profiles = handles.into_iter().flat_map(|h| h.join().expect("benchmark thread panicked")).collect();
            MultiTxMultiQueryProfile {
                name,
                profiles,
                run_descriptor: run_descriptor.clone(),
                total_wall_time: very_beginning.elapsed(),
            }
        })
    }

    fn new_runner_thread(database: Arc<Database<WALClient>>, producer: Arc<WorkloadInstance>, query: &'static str, run_descriptor: RunDescriptor) -> impl FnOnce() -> Vec<MultiQueryTxProfile>{
        let overestimate_txns_per_thread: usize =
            ((1.5 * run_descriptor.total_txns as f64 / run_descriptor.parallelism as f64).ceil() as usize).max(2);
        let local_profiles = Vec::with_capacity(overestimate_txns_per_thread);
        move || {
            let mut local_profiles = local_profiles;
            while producer.has_remaining() {
                let mut query_profiles = Vec::with_capacity(run_descriptor.n_queries_per_tx);
                let mut tx = TransactionWrite::open(database.clone(), transaction_options_with_profiling()).unwrap();
                let start = Instant::now();
                for _ in 0..run_descriptor.n_queries_per_tx {
                    let Some(given_rows) = producer.take_next_batch() else {
                        break;
                    };
                    let (query_result, tx_returned) = unpack_result(execute_write_query_in::<_, CountResults>(
                        tx,
                        query,
                        Some(given_rows),
                        true,
                    ));
                    tx = tx_returned;
                    let QueryAnswer { profile: query_profile, answer: rows } = query_result.unwrap();
                    assert_eq!(rows, run_descriptor.n_rows_per_query);
                    query_profiles.push(query_profile);
                }
                if !query_profiles.is_empty() {
                    let tx_profile = commit(tx).unwrap();
                    local_profiles.push(MultiQueryTxProfile {
                        tx_profile,
                        query_profiles,
                        time_elapsed: start.elapsed(),
                    });
                }
            }
            local_profiles
        }
    }
}

/// Pre-generated batches consumed work-stealing style.
/// Each slot is claimed by exactly one thread via fetch_add, so no locking needed.
pub struct WorkloadInstance {
    query: &'static str,
    variables: Vec<String>,

    batches: Vec<UnsafeCell<Option<GivenRowsSimple>>>,
    next_index: AtomicUsize,
}

// Safety: each index is claimed by exactly one thread (fetch_add ensures uniqueness).
unsafe impl Sync for WorkloadInstance {}

impl WorkloadInstance {
    fn build(query: &QueryDescriptor, run: &RunDescriptor) -> Arc<Self> {
        let mut rng = RandomDataGen::new();
        let batches = (0..(run.total_txns * run.n_queries_per_tx))
            .map(|_| {
                let given_rows_opt = query.produce_row.map(|produce| {
                    let rows = (0..run.n_rows_per_query).map(|_| produce(&mut rng)).collect();
                    GivenRowsSimple { variables: query.variables.clone(), rows }
                });
                UnsafeCell::new(given_rows_opt)
            })
            .collect();
        Arc::new(
            Self { query: query.query, variables: query.variables.clone(), batches, next_index: AtomicUsize::new(0) }
        )
    }

    pub fn query(&self) -> &str {
        self.query
    }

    pub fn variables(&self) -> &Vec<String> {
        &self.variables
    }

    pub fn has_remaining(&self) -> bool {
        self.next_index.load(Ordering::Relaxed) < self.batches.len()
    }

    pub fn take_next_batch(&self) -> Option<GivenRowsSimple> {
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        if idx >= self.batches.len() {
            return None;
        }
        // Safety: idx is unique — no other thread will ever access this slot.
        unsafe { (*self.batches[idx].get()).take() }
    }

    pub fn make_preload_data_fn(
        query: &'static str,
        variables: Vec<String>,
        produce_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
        n_total_rows: usize,
        n_rows_per_query: usize,
    ) -> PreloadDataFn {
        Box::new(move |database: Arc<Database<WALClient>>| {
            let mut rng = RandomDataGen::new();
            let mut remaining = n_total_rows;
            while remaining > 0 {
                let this_batch = n_rows_per_query.min(remaining);
                remaining -= this_batch;
                let rows = (0..this_batch).map(|_| produce_row(&mut rng)).collect();
                let given_rows = GivenRowsSimple { variables: variables.clone(), rows };
                let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
                let (result, tx) =
                    unpack_result(execute_write_query_in::<_, CountResults>(tx, query, Some(given_rows), false));
                let QueryAnswer { answer: n_rows, .. } = result.unwrap();
                commit(tx).unwrap();
            }
        })
    }
}

// Initial data
pub fn no_initial_data() -> Option<PreloadDataFn> {
    None
}

// prepare_run
pub fn no_given_rows() -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(|_: Arc<Database<WALClient>>| None)
}

pub fn n_empty_given_rows(n: usize) -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = Vec::new();
        let mut rows = Vec::with_capacity(n);
        rows.resize(n, Vec::new());
        Some(GivenRowsSimple { variables, rows })
    })
}

pub fn given_rows_with(
    n_rows: usize,
    variables: Vec<String>,
    gen_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
) -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = variables.clone();
        let mut rows = Vec::with_capacity(n_rows);
        let mut rng = RandomDataGen::new();
        let gen_row = move || gen_row(&mut rng);
        rows.resize_with(n_rows, gen_row);
        Some(GivenRowsSimple { variables, rows })
    })
}

pub fn query_in_write_tx(query: &str) -> BenchmarkedFn<Option<GivenRowsSimple>, TxQueryProfile> {
    let query_owned = query.to_owned();
    Box::new(move |database: Arc<Database<WALClient>>, given_rows: Option<GivenRowsSimple>| {
        let tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
        let (query_result, tx) =
            unpack_result(execute_write_query_in::<_, CountResults>(tx, query_owned.as_str(), given_rows, true));
        let QueryAnswer { profile: query_profile, answer: _ } = query_result.unwrap();
        let tx_profile = commit(tx).unwrap();
        TxQueryProfile { tx_profile: Some(tx_profile), query_profile }
    })
}
