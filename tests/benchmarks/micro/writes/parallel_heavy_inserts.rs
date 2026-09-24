/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    QueryAnswer, commit,
    datagen::RandomDataGen,
    execute_write_query_in,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    templates::{MultiQueryTxProfile, MultiTxMultiQueryProfile, PreloadDataFn, TypeDBMicroBenchmark, no_initial_data},
    utils::{CountResults, unpack_result},
};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

use crate::simple_inserts::SCHEMA as SIMPLE_SCHEMA;

pub(crate) struct GivenRowBatchProducer {
    pub query: &'static str,
    variables: Vec<String>,
    n_rows_per_query: usize,
    produce_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
    // One RNG per thread — indexed by thread_id, never shared, so locks are uncontended.
    rngs: Vec<Mutex<RandomDataGen>>,
    next_txn_index: AtomicUsize,
    n_total_txns: usize,
}

impl GivenRowBatchProducer {
    pub fn new(
        query: &'static str,
        variables: Vec<String>,
        n_rows_per_query: usize,
        n_total_txns: usize,
        n_parallel: usize,
        produce_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
    ) -> Self {
        let rngs = (0..n_parallel).map(|_| Mutex::new(RandomDataGen::new())).collect();
        Self {
            query,
            variables,
            n_rows_per_query,
            produce_row,
            rngs,
            next_txn_index: AtomicUsize::new(0),
            n_total_txns,
        }
    }

    pub fn get_next_batch(&self, thread_id: usize) -> Option<GivenRowsSimple> {
        let txn_index = self.next_txn_index.fetch_add(1, Ordering::Relaxed);
        if txn_index >= self.n_total_txns {
            return None;
        }
        let mut rng = self.rngs[thread_id].lock().unwrap();
        let rows = (0..self.n_rows_per_query).map(|_| (self.produce_row)(&mut rng)).collect();
        Some(GivenRowsSimple { variables: self.variables.clone(), rows })
    }
}

type HeavyInsertBenchmark = TypeDBMicroBenchmark<Arc<GivenRowBatchProducer>, MultiTxMultiQueryProfile>;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("parallel_inserts");
    group.run_benchmark(parallel_many_small_tx());
    group.run_benchmark(parallel_many_average_tx());
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
    produce_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
) -> HeavyInsertBenchmark {
    let benchmark_fn = Box::new(move |database: Arc<Database<WALClient>>, producer: Arc<GivenRowBatchProducer>| {
        let handles: Vec<_> = (0..n_parallel)
            .map(|thread_id| {
                let db = database.clone();
                let producer = producer.clone();
                std::thread::spawn(move || {
                    let mut local_profiles = Vec::new();
                    'outer: loop {
                        let start = Instant::now();
                        let mut query_profiles = Vec::with_capacity(n_query_per_txn);
                        let mut tx = TransactionWrite::open(db.clone(), TransactionOptions::default()).unwrap();
                        for _ in 0..n_query_per_txn {
                            let Some(given_rows) = producer.get_next_batch(thread_id) else {
                                // Exhausted mid-transaction: commit whatever ran and stop.
                                if !query_profiles.is_empty() {
                                    let tx_profile = commit(tx).unwrap();
                                    let time_elapsed = start.elapsed();
                                    local_profiles.push(MultiQueryTxProfile {
                                        tx_profile,
                                        query_profiles,
                                        time_elapsed,
                                    });
                                }
                                break 'outer;
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
                        let tx_profile = commit(tx).unwrap();
                        let time_elapsed = start.elapsed();
                        local_profiles.push(MultiQueryTxProfile { tx_profile, query_profiles, time_elapsed });
                    }
                    local_profiles
                })
            })
            .collect();

        let profiles = handles.into_iter().flat_map(|h| h.join().expect("benchmark thread panicked")).collect();

        MultiTxMultiQueryProfile { name, profiles }
    });

    TypeDBMicroBenchmark {
        name,
        schema,
        preload_data_fn,
        prepare_iter_fn: Box::new(move |_| {
            Arc::new(GivenRowBatchProducer::new(
                query,
                vec![],
                n_rows_per_query,
                n_txns * n_query_per_txn,
                n_parallel,
                produce_row,
            ))
        }),
        benchmark_fn,
    }
}

fn parallel_many_small_tx() -> HeavyInsertBenchmark {
    parametrised_insert(
        "parallel_many_small_tx",
        SIMPLE_SCHEMA,
        no_initial_data(),
        8,
        100_000,
        1,
        1,
        "given; insert $x isa person;",
        |_| vec![],
    )
}

fn parallel_many_average_tx() -> HeavyInsertBenchmark {
    parametrised_insert(
        "parallel_many_average_tx",
        SIMPLE_SCHEMA,
        no_initial_data(),
        8,
        1_000,
        1,
        100,
        "given; insert $x isa person;",
        |_| vec![],
    )
}

fn parallel_binary_relation() -> HeavyInsertBenchmark {
    let schema = r#"
    define
        relation r, relates e1, relates e2;
        entity e1, plays r:e1;
        entity e2, plays r:e1;
    "#;
    parametrised_insert(
        "parallel_binary_relation",
        schema,
        no_initial_data(),
        8,
        1_000,
        1,
        100,
        r#"
        given $e1:e1, $e2: e2, $e2_2: e2;
        insert
            $r isa r1, links (e1: $e1, e2: $e2);
            $r2 isa r1, links (e1: $e1, e2: $e2_2);
       "#,
        |_| vec![],
    )
}
