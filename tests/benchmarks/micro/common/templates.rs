/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Duration};

use database::{
    Database,
    transaction::{TransactionRead, TransactionWrite},
};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use resource::profile::{QueryProfile, TransactionProfile};
use storage::durability_client::WALClient;

use crate::{
    Config, Context, QueryAnswer, commit,
    datagen::RandomDataGen,
    execute_read_query_in, execute_write_query_in,
    utils::{CountResults, unpack_result},
};

/// See `BenchmarkRunner` implementations for the order in which these functions are called.
pub trait SimpleBenchmark {
    type IterInput;
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

    /// Create given_rows if needed
    fn prepare_iter(&self, context: &Context, database: Arc<Database<WALClient>>) -> Self::IterInput;

    /// The actual iteration which gets timed over and over again.
    fn run_iter(
        &self,
        context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::IterInput,
    ) -> Self::IterOutput;
}

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareIterFn<IN> = Box<dyn Fn(Arc<Database<WALClient>>) -> IN>;
pub type BenchmarkedFn<IN, OUT> = Box<dyn Fn(Arc<Database<WALClient>>, IN) -> OUT>;

pub trait SimpleReport {
    fn report(reports: &[Self])
    where
        Self: Sized;
}

impl SimpleReport for () {
    fn report(reports: &[Self]) {
        println!("DONE. [Report was (), which is a nop dummy].")
    }
}

pub struct TypeDBMicroBenchmark<IN, OUT: SimpleReport> {
    pub name: &'static str,
    pub schema: &'static str,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub prepare_iter_fn: PrepareIterFn<IN>,
    pub benchmark_fn: BenchmarkedFn<IN, OUT>,
}

impl<IN, OUT: SimpleReport> SimpleBenchmark for TypeDBMicroBenchmark<IN, OUT> {
    type IterInput = IN;
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

    fn prepare_iter(&self, _context: &Context, database: Arc<Database<WALClient>>) -> Self::IterInput {
        (self.prepare_iter_fn)(database)
    }

    fn run_iter(
        &self,
        _context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::IterInput,
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
        prepare_iter_fn: Box::new(|_| std::thread::sleep(Duration::from_millis(20))),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_millis(10))),
    }
}

// Util return
pub struct TxQueryProfile {
    pub tx_profile: Option<TransactionProfile>,
    pub query_profile: Arc<QueryProfile>,
}

pub struct MultiQueryTxProfile {
    pub tx_profile: TransactionProfile,
    pub query_profiles: Vec<Arc<QueryProfile>>,
    pub time_elapsed: Duration,
}

pub struct MultiTxMultiQueryProfile {
    pub name: &'static str,
    pub profiles: Vec<MultiQueryTxProfile>,
}

// Initial data
pub fn no_initial_data() -> Option<PreloadDataFn> {
    None
}

// prepare_iter
pub fn no_given_rows() -> PrepareIterFn<Option<GivenRowsSimple>> {
    Box::new(|_: Arc<Database<WALClient>>| None)
}

pub fn n_empty_given_rows(n: usize) -> PrepareIterFn<Option<GivenRowsSimple>> {
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
) -> PrepareIterFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = variables.clone();
        let mut rows = Vec::with_capacity(n_rows);
        let mut rng = RandomDataGen::new();
        let gen_row = move || gen_row(&mut rng);
        rows.resize_with(n_rows, gen_row);
        Some(GivenRowsSimple { variables, rows })
    })
}

// SimpleReport implementations
impl SimpleReport for TxQueryProfile {
    fn report(reports: &[Self]) {
        for (i, r) in reports.iter().enumerate() {
            let name = format!("tx_query_profile_{i}");
            crate::reports::TxQueryProfileReport::from(r).write_and_print(&name);
        }
    }
}

impl SimpleReport for MultiTxMultiQueryProfile {
    fn report(reports: &[Self]) {
        // Reports is a &[Self] but From consumes — clone timing data out into one merged report.
        // For simplicity, report each benchmark sample separately; typically there is only one.
        for r in reports.iter() {
            crate::reports::MultiQueryTxProfileReport::from_ref(r).write_and_print(r.name);
        }
    }
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
