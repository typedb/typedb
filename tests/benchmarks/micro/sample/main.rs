/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc, time::Duration};

use clap::Parser;
use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    query::QueryAnswer,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    transaction::UnifiedTransactionView,
    typedb_workload::{
        CountResults, benchmark::TypeDBMicroBenchmark, profiles::TxQueryProfile, reports::TxQueryProfileReport,
    },
};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

pub fn sanity_check() -> TypeDBMicroBenchmark<(), (), ()> {
    TypeDBMicroBenchmark {
        name: "sanity_check".to_owned(),
        schema: "define entity person;".to_owned(),
        preload_data_fn: Some(Box::new(|_| std::thread::sleep(Duration::from_millis(40)))),
        warmup_fn: None,
        prepare_run_fn: Box::new(|_| std::thread::sleep(Duration::from_millis(20))),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_millis(10))),
        _report: PhantomData,
    }
}

// TxQueryProfileReport is slightly less verbose than QueryFocusedReport
pub fn insert_million_entities() -> TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile, TxQueryProfileReport>
{
    let prepare_inputs_fn = Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = Vec::new();
        let rows = vec![vec![]; 1_000_000];
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
        benchmark_fn,
        _report: PhantomData,
    }
}

pub fn run_sample_benchmarks(runner: &mut impl BenchmarkRunner) {
    let mut sanity_checks = runner.new_group("sanity_checks");
    sanity_checks.run_benchmark(sanity_check());
    sanity_checks.run_benchmark(insert_million_entities());
}

fn main() {
    let mut runner = SimpleRunner::parse();
    run_sample_benchmarks(&mut runner);
    runner.summary();
}
