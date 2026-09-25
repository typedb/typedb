/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Instant};

use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    QueryAnswer,
    benchmark::{RunDescriptor, TypeDBMicroBenchmark},
    commit, execute_write_query_in,
    profiling::{MultiQueryTxProfile, MultiTxMultiQueryProfile},
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    utils::{CountResults, unpack_result},
};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

use crate::{given_rows_with, no_initial_data};

type HeavyInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, MultiTxMultiQueryProfile>;
pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("simple_inserts");
    group.run_benchmark(many_small_tx());
    group.run_benchmark(many_queries_per_tx());
    group.run_benchmark(single_large_query_per_tx());
    group.run_benchmark(ten_large_query_per_tx());
    group.run_benchmark(many_average_tx());
}

fn parametrised_entity_insert(
    name: &'static str,
    n_txns: usize,
    n_queries_per_tx: usize,
    n_entities_per_query: usize,
) -> HeavyInsertBenchmark {
    let query = "given; insert $x isa person;";
    let query_owned = query.to_owned();
    let run_descriptor =
        RunDescriptor { parallelism: 1, total_txns: n_txns, n_queries_per_tx, n_rows_per_query: n_entities_per_query };
    let benchmark_fn =
        Box::new(move |database: Arc<Database<WALClient>>, given_rows_to_clone: Option<GivenRowsSimple>| {
            let mut profiles = Vec::with_capacity(n_txns);
            let very_beginning = Instant::now();
            for _ in 0..n_txns {
                let start = Instant::now();
                let mut query_profiles = Vec::with_capacity(n_queries_per_tx);
                let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
                for _ in 0..n_queries_per_tx {
                    let given_rows = given_rows_to_clone
                        .as_ref()
                        .map(|g| GivenRowsSimple { variables: g.variables.clone(), rows: g.rows.clone() });
                    let (query_result, tx_returned) = unpack_result(execute_write_query_in::<_, CountResults>(
                        tx,
                        query_owned.as_str(),
                        given_rows,
                        true,
                    ));
                    tx = tx_returned;
                    let QueryAnswer { profile: query_profile, answer: rows } = query_result.unwrap();
                    assert_eq!(rows, n_entities_per_query);
                    query_profiles.push(query_profile);
                }
                let tx_profile = commit(tx).unwrap();
                let time_elapsed = start.elapsed();
                profiles.push(MultiQueryTxProfile { tx_profile, query_profiles, time_elapsed });
            }
            MultiTxMultiQueryProfile {
                name,
                profiles,
                run_descriptor: run_descriptor.clone(),
                total_wall_time: very_beginning.elapsed(),
            }
        });
    TypeDBMicroBenchmark {
        name,
        schema: crate::simple_inserts::SCHEMA,
        warmup_fn: None, // TODO
        preload_data_fn: no_initial_data(),
        prepare_run_fn: given_rows_with(n_entities_per_query, vec![], |_| vec![]),
        benchmark_fn,
    }
}

// Instantiations
fn many_small_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("many_small_tx", 100_000, 1, 1)
}

fn many_queries_per_tx() -> HeavyInsertBenchmark {
    parametrised_entity_insert("many_queries_per_tx", 100, 1000, 1)
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
