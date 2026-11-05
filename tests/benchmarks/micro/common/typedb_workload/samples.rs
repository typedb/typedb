/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc, time::Duration};

use database::{Database, transaction::TransactionWrite};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

use crate::{
    query::QueryAnswer,
    transaction::UnifiedTransactionView,
    typedb_workload::{
        CountResults, benchmark::TypeDBMicroBenchmark, profiles::TxQueryProfile, reports::TxQueryProfileReport,
    },
};

pub fn sanity_check() -> TypeDBMicroBenchmark<(), (), ()> {
    // Just to ensure the reported time is just the benchmark_fn
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

pub fn insert_million_entities() -> TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile, TxQueryProfileReport>
{
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
