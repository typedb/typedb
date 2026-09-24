/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, time::Instant};

use database::{Database, transaction::TransactionWrite};
use encoding::graph::type_::vertex::TypeID;
use lib_benchmark::{
    QueryAnswer,
    benchmark::{PreloadDataFn, RunDescriptor, TypeDBMicroBenchmark, WorkloadInstance, no_initial_data},
    commit,
    datagen::RandomDataGen,
    execute_write_query_in,
    profiling::{MultiQueryTxProfile, MultiTxMultiQueryProfile, transaction_options_with_profiling},
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    utils::{CountResults, unpack_result},
};
use query::given_rows::GivenRowEntry;
use storage::durability_client::WALClient;

use crate::simple_inserts::SCHEMA as SIMPLE_SCHEMA;

type ParallelHeavyInsertBenchmark = TypeDBMicroBenchmark<Arc<WorkloadInstance>, MultiTxMultiQueryProfile>;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("parallel_inserts");
    group.run_benchmark(parallel_many_small_tx());
    group.run_benchmark(parallel_many_average_tx());
    group.run_benchmark(parallel_many_large_tx());
    group.run_benchmark(parallel_binary_relation());
}

fn parametrised_insert(
    name: &'static str,
    schema: &'static str,
    preload_data_fn: Option<PreloadDataFn>,
    n_parallel: usize,
    n_txns: usize,
    n_query_per_txn: usize,
    n_rows_per_query: usize,
    query: &'static str,
    variables: Vec<String>,
    produce_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
) -> ParallelHeavyInsertBenchmark {
    let run_descriptor = RunDescriptor { total_txns: n_txns, n_queries_per_tx: n_query_per_txn, n_rows_per_query };
    let benchmark_fn = Box::new(move |database: Arc<Database<WALClient>>, producer: Arc<WorkloadInstance>| {
        let overestimate_txns_per_thread: usize = ((1.5 * n_txns as f64 / n_parallel as f64).ceil() as usize).max(2);
        let very_beginning = Instant::now();
        let handles: Vec<_> = (0..n_parallel)
            .map(|_thread_id| {
                let db = database.clone();
                let producer = producer.clone();
                let local_profiles = Vec::with_capacity(overestimate_txns_per_thread);
                std::thread::spawn(move || {
                    let mut local_profiles = local_profiles;
                    while producer.has_remaining() {
                        let mut query_profiles = Vec::with_capacity(n_query_per_txn);
                        let mut tx = TransactionWrite::open(db.clone(), transaction_options_with_profiling()).unwrap();
                        let start = Instant::now();
                        for _ in 0..n_query_per_txn {
                            let Some(given_rows) = producer.take_next_batch() else {
                                break;
                            };
                            let (query_result, tx_returned) = unpack_result(execute_write_query_in::<_, CountResults>(
                                tx,
                                query,
                                Some(given_rows),
                                true,
                            ));
                            tx = tx_returned;
                            let QueryAnswer { profile: query_profile, answer: rows } = query_result.unwrap();
                            assert_eq!(rows, n_rows_per_query);
                            query_profiles.push(query_profile);
                        }
                        if !query_profiles.is_empty() {
                            let tx_profile = commit(tx).unwrap();
                            local_profiles.push(MultiQueryTxProfile {
                                tx_profile,
                                query_profiles,
                                time_elapsed: start.elapsed(),
                            });
                        }
                    }
                    local_profiles
                })
            })
            .collect();

        let profiles = handles.into_iter().flat_map(|h| h.join().expect("benchmark thread panicked")).collect();
        MultiTxMultiQueryProfile {
            name,
            profiles,
            run_descriptor: run_descriptor.clone(),
            total_wall_time: very_beginning.elapsed(),
        }
    });

    TypeDBMicroBenchmark { name, schema, preload_data_fn, warmup_fn: todo!(), prepare_run_fn: todo!(), benchmark_fn }
}

fn parallel_many_small_tx() -> ParallelHeavyInsertBenchmark {
    parametrised_insert(
        "parallel_many_small_tx",
        SIMPLE_SCHEMA,
        no_initial_data(),
        8,
        100_000,
        1,
        1,
        "given; insert $x isa person;",
        vec![],
        |_| vec![],
    )
}

fn parallel_many_average_tx() -> ParallelHeavyInsertBenchmark {
    parametrised_insert(
        "parallel_many_average_tx",
        SIMPLE_SCHEMA,
        no_initial_data(),
        8,
        1_000,
        1,
        100,
        "given; insert $x isa person;",
        vec![],
        |_| vec![],
    )
}

fn parallel_many_large_tx() -> ParallelHeavyInsertBenchmark {
    parametrised_insert(
        "parallel_many_large_tx",
        SIMPLE_SCHEMA,
        no_initial_data(),
        8,
        1_000,
        1,
        10_000,
        "given; insert $x isa person;",
        vec![],
        |_| vec![],
    )
}

fn parallel_binary_relation() -> ParallelHeavyInsertBenchmark {
    const N_ENTITIES: usize = 100_000;
    let schema = r#"
    define
        relation r1, relates e1, relates e2;
        entity e1, plays r1:e1;
        entity e2, plays r1:e2;
    "#;
    let preload_data_fn = WorkloadInstance::make_preload_data_fn(
        "given; insert $_ isa e1; $_ isa e2;",
        vec![],
        |_| vec![],
        N_ENTITIES,
        10_000,
    );

    let produce_row = |rng: &mut RandomDataGen| {
        vec![
            rng.entry_entity_raw_in(TypeID::new(0), 0, (N_ENTITIES - 1) as u64),
            rng.entry_entity_raw_in(TypeID::new(1), 0, (N_ENTITIES - 1) as u64),
        ]
    };
    parametrised_insert(
        "parallel_binary_relation",
        schema,
        Some(preload_data_fn),
        16,
        1_000,
        1,
        1_000,
        r#"
        given $e1:e1, $e2: e2;
        insert
            $r isa r1, links (e1: $e1, e2: $e2);
       "#,
        vec!["e1".to_owned(), "e2".to_owned()],
        produce_row,
    )
}
