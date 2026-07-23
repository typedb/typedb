/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use lib_benchmark::{
    profiler::FlamegraphProfiler,
    templates::{SimpleBenchmark, TypeDBMicroBenchmark},
};
use query::given_rows::GivenRowsSimple;

mod simple_inserts;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>>;

fn sanity_check() -> TransactionInsertBenchmark {
    // Just to ensure the reported time is just the benchmark_fn
    TransactionInsertBenchmark {
        name: "sanity_check",
        schema: "define entity person;",
        preload_data_fn: Some(Box::new(|_| std::thread::sleep(Duration::from_secs(4)))),
        prepare_iter_fn: Box::new(|_| {
            std::thread::sleep(Duration::from_secs(2));
            None
        }),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_secs(1))),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    sanity_check().run_benchmark(&mut c.benchmark_group("sanity_check").sample_size(10));
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
