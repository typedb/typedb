use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use database::{Database, transaction::TransactionWrite};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

use crate::{
    Context, QueryAnswer,
    benchmark::{SimpleBenchmark, SimpleReport},
    commit, execute_write_query_in,
    typedb_workload::{
        MultiQueryTxProfile, MultiTxMultiQueryProfile, QueryDescriptor, RunDescriptor, datagen::RandomDataGen,
        reports::QueryFocusedReport, transaction_options_with_profiling,
    },
    utils::{CountResults, unpack_result},
};

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type WarmupFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareRunFn<IN> = Box<dyn Fn(Arc<Database<WALClient>>) -> IN>;
pub type BenchmarkedFn<IN, OUT> = Box<dyn Fn(Arc<Database<WALClient>>, IN) -> OUT>;

pub struct TypeDBMicroBenchmark<IN, OUT, REPORT: SimpleReport<OUT>> {
    pub name: String,
    pub schema: String,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub warmup_fn: Option<WarmupFn>,
    pub prepare_run_fn: PrepareRunFn<IN>,
    pub benchmark_fn: BenchmarkedFn<IN, OUT>,
    pub _report: PhantomData<REPORT>,
}

impl<IN, OUT, REPORT: SimpleReport<OUT>> SimpleBenchmark for TypeDBMicroBenchmark<IN, OUT, REPORT> {
    type RunInput = IN;
    type IterOutput = OUT;
    type Report = REPORT;

    fn name(&self) -> &'_ str {
        self.name.as_str()
    }

    fn prepare_database(&self, _context: &Context, database: Arc<Database<WALClient>>) {
        crate::create_schema(database.clone(), self.schema.as_str());
        if let Some(preload_fn) = &self.preload_data_fn {
            preload_fn(database.clone());
        }
        database.benchmark_only_flush();
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

pub fn sanity_check() -> TypeDBMicroBenchmark<(), (), ()> {
    // Just to ensure the reported time is just the benchmark_fn
    TypeDBMicroBenchmark {
        name: "sanity_check".to_owned(),
        schema: "define entity person;".to_owned(),
        preload_data_fn: Some(Box::new(|_| std::thread::sleep(Duration::from_millis(40)))),
        warmup_fn: None,
        prepare_run_fn: Box::new(|_| std::thread::sleep(Duration::from_millis(20))),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_millis(10))),
        _report: PhantomData,
    }
}

impl<Report: SimpleReport<MultiTxMultiQueryProfile>> TypeDBWorkloadBenchmark<Report> {
    pub fn new(
        name: impl Into<String>,
        schema: impl Into<String>,
        preload_data_fn: Option<PreloadDataFn>,
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
    ) -> Self {
        let name: String = name.into();
        let schema: String = schema.into();
        let warmup_fn = query_descriptor.for_warmup();
        let workload = WorkLoad { query_descriptor, run_descriptor };
        let prepare_run_fn = workload.prepare_fn();
        let benchmark_fn = workload.runner(name.clone());
        Self { name, schema, preload_data_fn, warmup_fn, prepare_run_fn, benchmark_fn, _report: PhantomData }
    }
}

pub type TypeDBWorkloadBenchmark<Report> =
    TypeDBMicroBenchmark<Arc<WorkloadInstance>, MultiTxMultiQueryProfile, Report>;
pub type TypeDBQueryWorkloadBenchmark = TypeDBWorkloadBenchmark<QueryFocusedReport>;

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

    pub fn runner(&self, name: impl Into<String>) -> BenchmarkedFn<Arc<WorkloadInstance>, MultiTxMultiQueryProfile> {
        let name: String = name.into();
        let query: Arc<String> = Arc::new(self.query_descriptor.query.clone());
        let run_descriptor = self.run_descriptor.clone();
        Box::new(move |database: Arc<Database<WALClient>>, producer: Arc<WorkloadInstance>| {
            let very_beginning = Instant::now();
            let handles: Vec<_> = (0..run_descriptor.parallelism)
                .map(|_thread_id| {
                    let database = database.clone();
                    let producer = producer.clone();
                    let run_descriptor = run_descriptor.clone();
                    std::thread::spawn(Self::new_runner_thread(database, producer, query.clone(), run_descriptor))
                })
                .collect();

            let profiles = handles.into_iter().flat_map(|h| h.join().expect("benchmark thread panicked")).collect();
            MultiTxMultiQueryProfile {
                name: name.clone(),
                profiles,
                run_descriptor: run_descriptor.clone(),
                total_wall_time: very_beginning.elapsed(),
            }
        })
    }

    fn new_runner_thread(
        database: Arc<Database<WALClient>>,
        producer: Arc<WorkloadInstance>,
        query: Arc<String>,
        run_descriptor: RunDescriptor,
    ) -> impl FnOnce() -> Vec<MultiQueryTxProfile> {
        let overestimate_txns_per_thread: usize =
            ((1.5 * run_descriptor.total_txns as f64 / run_descriptor.parallelism as f64).ceil() as usize).max(2);
        let local_profiles = Vec::with_capacity(overestimate_txns_per_thread);
        let query: Arc<String> = query.clone();
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
                        query.as_str(),
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
    query: String,
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
        Arc::new(Self {
            query: query.query.clone(),
            variables: query.variables.clone(),
            batches,
            next_index: AtomicUsize::new(0),
        })
    }

    pub fn query(&self) -> &str {
        self.query.as_str()
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
        query: String,
        variables: Vec<String>,
        produce_row: fn(usize, &mut RandomDataGen) -> Vec<GivenRowEntry>,
        n_total_rows: usize,
        n_rows_per_query: usize,
    ) -> PreloadDataFn {
        Box::new(move |database: Arc<Database<WALClient>>| {
            let mut rng = RandomDataGen::new();
            let mut completed = 0;
            while completed < n_total_rows {
                let this_batch = n_rows_per_query.min(n_total_rows - completed);
                let rows = (0..this_batch).map(|i| produce_row(completed + i, &mut rng)).collect();
                let given_rows = GivenRowsSimple { variables: variables.clone(), rows };
                let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
                let (result, tx) = unpack_result(execute_write_query_in::<_, CountResults>(
                    tx,
                    query.as_str(),
                    Some(given_rows),
                    false,
                ));
                let QueryAnswer { answer: n_answer_rows, .. } = result.unwrap();
                assert_eq!(n_answer_rows, n_rows_per_query);
                commit(tx).unwrap();
                completed += this_batch;
            }
        })
    }
}
