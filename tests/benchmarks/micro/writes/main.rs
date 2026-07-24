/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::Criterion;
use itertools::Itertools;
use lib_benchmark::{
    profiler::FlamegraphProfiler,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    templates::{SimpleBenchmark, TxQueryProfile, TypeDBMicroBenchmark, sanity_check},
};
use query::given_rows::GivenRowsSimple;

mod simple_inserts;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    runner.new_group("sanity_check").run_benchmark(sanity_check());
    simple_inserts::run_all(&mut runner);
    runner.summary();
}

fn criterion_runner() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100)).configure_from_args()
}

fn simple_runner() -> SimpleRunner {
    let args = std::env::args().collect::<Vec<_>>();
    debug_assert!(args.len() > 2 && args[1].as_str() == "--simple");
    let filter = args.get(2).cloned().unwrap_or_else(|| "".to_owned());
    SimpleRunner::new(filter)
}

fn main() {
    // TODO: Can switch between others
    if Some("--simple") == std::env::args().skip(1).next().as_ref().map(|x| x.as_str()) {
        run_benchmarks(simple_runner())
    } else {
        run_benchmarks(criterion_runner())
    };
}
