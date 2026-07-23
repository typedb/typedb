/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    Context,
    profiler::FlamegraphProfiler,
    templates::SimpleBenchmark,
    utils::{ResultCounter, unpack_result},
};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

mod simple_inserts;

struct InsertBenchmark {
    pub(crate) name: &'static str,
    pub(crate) schema: &'static str,
    pub(crate) query: &'static str,
    pub(crate) given_rows: Option<GivenRowsSimple>,
}

impl InsertBenchmark {
    fn new(name: &'static str, schema: &'static str, query: &'static str, given_rows: Option<GivenRowsSimple>) -> Self {
        Self { name, schema, query, given_rows }
    }
}

impl SimpleBenchmark for InsertBenchmark {
    type IterInput = Arc<Database<WALClient>>;

    fn name(&self) -> String {
        self.name.to_owned()
    }

    fn prepare(&self, context: &Context) -> Self::IterInput {
        let database = context.recreate_database(self.name).unwrap();
        lib_benchmark::create_schema(database.clone(), self.schema);
        database
    }

    fn run(&self, _context: &Context, database: &mut Arc<Database<WALClient>>) {
        let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        let (query_result, tx) =
            unpack_result(lib_benchmark::execute_write_query_in::<_, ResultCounter>(tx, self.query, None));
        query_result.unwrap();
        lib_benchmark::commit(tx).unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    simple_inserts::run_all(c);
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100))
}

criterion_group!(
    name = benches;
    config = profiled();
    targets = criterion_benchmark
);

criterion_main!(benches);
