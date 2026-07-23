/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkGroup, measurement::Measurement};
use database::{Database, transaction::TransactionWrite};
use itertools::repeat_n;
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

use crate::{
    Config, Context, commit, execute_write_query_in,
    utils::{CountResults, unpack_result},
};

pub trait SimpleBenchmark {
    type IterInput;

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
    fn run_iter(&self, context: &Context, database: Arc<Database<WALClient>>, input: Self::IterInput);

    /// Prepares & runs the iters. Abstracts away criterion so we don't make mistakes in the setup.
    fn run_benchmark<M: Measurement>(&self, group: &mut BenchmarkGroup<M>) {
        let mut context = self.init_context();
        self.before_all(&mut context);
        group.bench_function(self.name(), |b| {
            // This should also be run only once per "batch"
            // We create the database outside the batch creation so the Arc isn't dropped in the timed part
            let database = self.create_database(&mut context);
            self.prepare_database(&mut context, database.clone());
            b.iter_batched(
                || self.prepare_iter(&context, database.clone()),
                |input| self.run_iter(&context, database.clone(), input),
                BatchSize::PerIteration,
            );
            drop(database);
        });
    }
}

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareIterFn<T> = Box<dyn Fn(Arc<Database<WALClient>>) -> T>;
pub type BenchmarkedFn<T> = Box<dyn Fn(Arc<Database<WALClient>>, T)>;

pub struct TypeDBMicroBenchmark<T> {
    pub name: &'static str,
    pub schema: &'static str,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub prepare_iter_fn: PrepareIterFn<T>,
    pub benchmark_fn: BenchmarkedFn<T>,
}

impl<T> SimpleBenchmark for TypeDBMicroBenchmark<T> {
    type IterInput = T;

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

    fn run_iter(&self, _context: &Context, database: Arc<Database<WALClient>>, input: Self::IterInput) {
        (self.benchmark_fn)(database, input)
    }
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

// queries
pub fn query_in_write_tx(query: &str) -> BenchmarkedFn<Option<GivenRowsSimple>> {
    let query_owned = query.to_owned();
    Box::new(move |database: Arc<Database<WALClient>>, given_rows: Option<GivenRowsSimple>| {
        let tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
        let (result, tx) =
            unpack_result(execute_write_query_in::<_, CountResults>(tx, query_owned.as_str(), given_rows));
        result.unwrap();
        commit(tx).unwrap();
    })
}
