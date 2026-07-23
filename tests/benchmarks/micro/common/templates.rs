/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use criterion::{BatchSize, BenchmarkGroup, measurement::Measurement};
use database::Database;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;
use crate::{Config, Context};

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
            b.iter_batched(
                || self.prepare_iter(&context, database.clone()),
                |input| self.run_iter(&context, database.clone(), input),
                BatchSize::PerIteration,
            );
            drop(database);
        });
    }
}

pub(crate) type PreloadDataFn = fn(Arc<Database<WALClient>>);
pub(crate) type PrepareIterFn<T> = fn(Arc<Database<WALClient>>) -> T;
pub(crate) type BenchmarkedFn<T> = fn(Arc<Database<WALClient>>, input: T);
struct TypeDBMicroBenchmark<T> {
    name: &'static str,
    schema: &'static str,
    preload_data_fn: Option<PreloadDataFn>,
    prepare_iter_fn: PrepareIterFn<T>,
    benchmark_fn: BenchmarkedFn<T>,
}

impl<T> TypeDBMicroBenchmark<T> {
    fn new(name: &'static str, schema: &'static str, preload_data_fn: Option<PreloadDataFn>, prepare_iter_fn: PrepareIterFn<T>, benchmark_fn: BenchmarkedFn<T>) -> Self {
        Self { name, schema, preload_data_fn, prepare_iter_fn, benchmark_fn }
    }
}

impl<T> SimpleBenchmark for TypeDBMicroBenchmark<T> {
    type IterInput = T;

    fn name(&self) -> &'_ str {
        self.name
    }

    fn prepare_database(&self, _context: &Context, database: Arc<Database<WALClient>>) {
        crate::create_schema(database.clone(), self.schema);
        if let Some(preload_fn) = self.preload_data_fn {
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
