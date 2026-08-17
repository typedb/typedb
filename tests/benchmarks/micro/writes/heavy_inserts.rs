/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Instant};
use database::Database;
use database::transaction::TransactionWrite;
use lib_benchmark::{commit, execute_write_query_in, runner::{BenchmarkRunner, BenchmarkRunnerGroup}, templates::{given_rows_with, n_empty_given_rows, no_given_rows, no_initial_data, query_in_write_tx}, QueryAnswer};
use lib_benchmark::templates::{MultiQueryTxProfile, MultiTxMultiQueryProfile, TxQueryProfile, TypeDBMicroBenchmark};
use lib_benchmark::utils::{unpack_result, CountResults};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

type HeavyInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, MultiTxMultiQueryProfile>;
pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("simple_inserts");
    group.run_benchmark(many_small_tx());
    group.run_benchmark(many_queries_per_tx());
    group.run_benchmark(single_large_query_per_tx());
    group.run_benchmark(ten_large_query_per_tx());
    group.run_benchmark(many_average_tx());
}

fn parametrised_entity_insert(name: &'static str, n_txns: usize, n_query_per_txn: usize, n_entities_per_query: usize) -> HeavyInsertBenchmark {
    let query = "given; insert $x isa person;";
    let query_owned = query.to_owned();
    let benchmark_fn = Box::new(move |database: Arc<Database<WALClient>>, given_rows_to_clone: Option<GivenRowsSimple>| {
        let mut profiles = Vec::with_capacity(n_txns);
        for _ in 0..n_txns {
            let start = Instant::now();
            let mut query_profiles = Vec::with_capacity(n_query_per_txn);
            let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
            for _ in 0..n_query_per_txn {
                let given_rows = given_rows_to_clone.as_ref().map(|g| {
                    GivenRowsSimple { variables: g.variables.clone(), rows: g.rows.clone() }
                });
                let (query_result, tx_returned) =
                    unpack_result(execute_write_query_in::<_, CountResults>(tx, query_owned.as_str(), given_rows, true));
                tx = tx_returned;
                let QueryAnswer { profile: query_profile, answer: rows } = query_result.unwrap();
                assert_eq!(rows, n_entities_per_query);
                query_profiles.push(query_profile);
            }
            let tx_profile = commit(tx).unwrap();
            let time_elapsed = start.elapsed();
            profiles.push(MultiQueryTxProfile { tx_profile, query_profiles, time_elapsed });
        }
        MultiTxMultiQueryProfile { profiles }
    });
    TypeDBMicroBenchmark {
        name,
        schema: crate::simple_inserts::SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: given_rows_with(n_entities_per_query, vec![], |_| vec![]),
        benchmark_fn,
    }
}

// Instantiations
fn many_small_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("many_small_tx", 100_000, 1, 1)
}

fn many_queries_per_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("many_queries_per_tx",100, 1000, 1)
}

fn single_large_query_per_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("single_large_query_per_tx", 100, 1, 10_000)
}

fn ten_large_query_per_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("ten_large_query_per_tx", 100, 10, 10_000)
}

fn many_average_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("many_average_tx", 1_000, 1, 100)
}
