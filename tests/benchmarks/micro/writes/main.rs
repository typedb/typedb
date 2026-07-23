/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::{Criterion, criterion_group, criterion_main};
use lib_benchmark::{
    profiler::FlamegraphProfiler,
    templates::{SimpleBenchmark, TypeDBMicroBenchmark},
};
use query::given_rows::GivenRowsSimple;

mod simple_inserts;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>>;

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
