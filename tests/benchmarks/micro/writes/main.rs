/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use clap::Parser;
use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    query::QueryAnswer,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    transaction::UnifiedTransactionView,
    typedb_workload::{
        CountResults, RunDescriptor,
        benchmark::{DbgAssertFn, PreloadDataFn, TypeDBMicroBenchmark},
        profiles::TxQueryProfile,
        reports::TxQueryProfileReport,
        run_configs::{CI_MINIMAL, PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL},
        sanity_check,
    },
};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

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

fn insert_million_entities() -> TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile, TxQueryProfileReport> {
    let prepare_inputs_fn = Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = Vec::new();
        let mut rows = vec![vec![]; 1_000_000];
        Some(GivenRowsSimple { variables, rows })
    });
    let benchmark_fn = Box::new(move |database: Arc<Database<WALClient>>, given_rows: Option<GivenRowsSimple>| {
        let tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
        let query = "given ; insert $x isa person;";
        let (query_result, tx) = tx.query::<CountResults>(query, given_rows, true);
        let QueryAnswer { profile: query_profile, answer: _ } = query_result.unwrap();
        let tx_profile = tx.commit().unwrap();
        TxQueryProfile { tx_profile: Some(tx_profile), query_profile }
    });
    TypeDBMicroBenchmark {
        name: "sanity_check_insert_million_entities".to_owned(),
        schema: "define entity person;".to_owned(),
        preload_data_fn: None,
        warmup_fn: None,
        prepare_run_fn: prepare_inputs_fn,
        benchmark_fn: benchmark_fn,
        _report: PhantomData,
    }
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
