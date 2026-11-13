/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use clap::Parser;
use lib_benchmark::{
    runner::{BenchmarkRunner, SimpleRunner},
    typedb_workload::{
        RunDescriptor,
        benchmark::DbgAssertFn,
        run_configs::{ROWS_LARGE, THREADS_PARALLEL, THREADS_SERIAL, TRANSACTIONS_MANY},
    },
};

mod constraints;
mod functions;
mod subpatterns;

const SERIAL_RUN_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};

const PARALLEL_RUN_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};

const SCALE_SMALL: usize = 1_000;
const SCALE_MEDIUM: usize = 50_000;
const SCALE_LARGE: usize = 1_000_000;

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
