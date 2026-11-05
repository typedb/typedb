/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use clap::Parser;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    transaction::UnifiedTransactionView,
    typedb_workload::{
        RunDescriptor,
        benchmark::{DbgAssertFn, PreloadDataFn},
        run_configs::{CI_MINIMAL, PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL},
        samples::{insert_million_entities, sanity_check},
    },
};

mod insert_entities;
mod insert_ownerships;
mod insert_relations;

const NO_INITIAL_DATA: Option<PreloadDataFn> = None;

const CI_RUNS: [RunDescriptor; 1] = [CI_MINIMAL];
const STANDARD_RUNS: [RunDescriptor; 4] =
    [SERIAL_MANY_SMALL, SERIAL_FEW_LARGE, PARALLEL_MANY_SMALL, PARALLEL_MANY_LARGE];
const SELECTED_RUN_DESCRIPTORS: &'static [RunDescriptor] = &STANDARD_RUNS;

pub(crate) fn assert_query_row_count_is(run_descriptor: &RunDescriptor) -> DbgAssertFn {
    let n_rows_per_query = run_descriptor.n_rows_per_query;
    Arc::new(move |actual_row_count: usize| assert_eq!(n_rows_per_query, actual_row_count))
}

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    run_sample_benchmarks(&mut runner);
    insert_entities::run_all(&mut runner);
    insert_ownerships::run_all(&mut runner);
    insert_relations::run_all(&mut runner);
    runner.summary();
}

// Samples for illustration:
fn run_sample_benchmarks(runner: &mut impl BenchmarkRunner) {
    let mut sanity_checks = runner.new_group("sanity_checks");
    sanity_checks.run_benchmark(sanity_check());
    sanity_checks.run_benchmark(insert_million_entities());
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
