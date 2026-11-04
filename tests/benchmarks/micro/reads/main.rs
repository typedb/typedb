/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use clap::Parser;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    typedb_workload::{
        RunDescriptor,
        benchmark::{DbgAssertFn, PreloadDataFn},
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

pub(crate) fn assert_query_row_count_within(expected_rows: f64, allowed_percent_difference: f64) -> DbgAssertFn {
    Arc::new(move |rows: usize| {
        let actual_percent_difference = (rows as i64 - expected_rows as i64).abs() as f64 / expected_rows as f64;
        assert!(actual_percent_difference < allowed_percent_difference)
    })
}

pub(crate) fn assert_query_row_count_is(run_descriptor: &RunDescriptor) -> DbgAssertFn {
    let n_rows_per_query = run_descriptor.n_rows_per_query;
    Arc::new(move |actual_row_count: usize| assert_eq!(n_rows_per_query, actual_row_count))
}

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    constraints::run_all(&mut runner);
    subpatterns::run_all(&mut runner);
    functions::run_all(&mut runner);
    runner.summary();
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
