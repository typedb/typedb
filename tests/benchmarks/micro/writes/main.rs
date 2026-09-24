/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use criterion::Criterion;
use lib_benchmark::{
    profiler::FlamegraphProfiler,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    templates::{SimpleBenchmark, TxQueryProfile, TypeDBMicroBenchmark, sanity_check},
};
use query::given_rows::GivenRowsSimple;

mod heavy_inserts;
// mod match_reads;
mod parallel_heavy_inserts;
mod simple_inserts;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    runner.new_group("sanity_check").run_benchmark(sanity_check());
    simple_inserts::run_all(&mut runner);
    heavy_inserts::run_all(&mut runner);
    parallel_heavy_inserts::run_all(&mut runner);
    // match_reads::run_all(&mut runner);

    runner.summary();
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(clap::Subcommand)]
enum Mode {
    /// Run with the simple (non-criterion) runner.
    Simple(SimpleRunner),
    /// Run with criterion (default benchmarking mode).
    Criterion,
}

fn main() {
    match Args::parse().mode {
        Mode::Simple(runner) => run_benchmarks(runner),
        Mode::Criterion => {
            run_benchmarks(Criterion::default().with_profiler(FlamegraphProfiler::new(100)).configure_from_args())
        }
    }
}
