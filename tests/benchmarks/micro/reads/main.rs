/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    typedb_workload::{
        RunDescriptor,
        benchmark::PreloadDataFn,
        run_configs::{CI_MINIMAL, PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL},
    },
};
mod constraints;
mod functions;
mod subpatterns;

const NO_INITIAL_DATA: Option<PreloadDataFn> = None;

const CI_RUNS: [RunDescriptor; 1] = [CI_MINIMAL];
const STANDARD_RUNS: [RunDescriptor; 4] =
    [SERIAL_MANY_SMALL, SERIAL_FEW_LARGE, PARALLEL_MANY_SMALL, PARALLEL_MANY_LARGE];
const SELECTED_RUN_DESCRIPTORS: &'static [RunDescriptor] = &STANDARD_RUNS;
const PRELOAD_SCALING_FACTOR: usize = 5;

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    constraints::run_all(&mut runner);
    subpatterns::run_all(&mut runner);
    functions::run_all(&mut runner);
    runner.summary();
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
