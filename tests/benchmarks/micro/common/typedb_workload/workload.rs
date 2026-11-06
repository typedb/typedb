/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use database::{Database, transaction::TransactionWrite};
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;

use crate::{
    query::QueryAnswer,
    transaction::UnifiedTransactionView,
    typedb_workload::{
        CountResults, QueryDescriptor, RunDescriptor,
        benchmark::{BenchmarkedFn, DbgAssertFn, PrepareRunFn, WarmupFn},
        datagen::RandomDataGen,
        profiles::{MultiQueryTxProfile, MultiTxMultiQueryProfile},
        transaction_options_with_profiling,
    },
};

pub(super) struct WorkLoad {
    pub(super) query_descriptor: QueryDescriptor,
    pub(super) run_descriptor: RunDescriptor,
    pub(super) dbg_assert: Option<DbgAssertFn>,
}

impl WorkLoad {
    pub(super) fn warmup_fn_for(query_descriptor: &QueryDescriptor) -> WarmupFn {
        let warmup_workload = WorkLoad {
            query_descriptor: query_descriptor.clone(),
            run_descriptor: RunDescriptor::WARMUP.clone(),
            dbg_assert: None,
        };
        let runner = warmup_workload.runner::<TransactionWrite<WALClient>>("_warmup");
        let prepare_fn = warmup_workload.prepare_fn();
        Box::new(move |db| {
            let db_clone = db.clone();
            runner(db, prepare_fn(db_clone));
        })
    }

    pub(super) fn prepare_fn(&self) -> PrepareRunFn<Arc<WorkloadInputBatches>> {
        let query_descriptor = self.query_descriptor.clone();
        let run_descriptor = self.run_descriptor.clone();
        Box::new(move |_| WorkloadInputBatches::build(&query_descriptor, &run_descriptor))
    }

    pub(super) fn runner<TxnType: UnifiedTransactionView>(
        &self,
        name: impl Into<String>,
    ) -> BenchmarkedFn<Arc<WorkloadInputBatches>, MultiTxMultiQueryProfile> {
        let name: String = name.into();
        let query: Arc<String> = Arc::new(self.query_descriptor.query.clone());
        let run_descriptor = self.run_descriptor.clone();
        let tx_type = self.query_descriptor.tx_type;
        let dbg_assert_to_clone = self.dbg_assert.clone();
        Box::new(move |database: Arc<Database<WALClient>>, producer: Arc<WorkloadInputBatches>| {
            let very_beginning = Instant::now();
            let handles: Vec<_> = (0..run_descriptor.parallelism)
                .map(|_thread_id| {
                    let database = database.clone();
                    let producer = producer.clone();
                    let run_descriptor = run_descriptor.clone();
                    let dbg_assert = dbg_assert_to_clone.clone();
                    std::thread::spawn(Self::new_runner_thread::<TxnType>(
                        database,
                        producer,
                        query.clone(),
                        run_descriptor,
                        dbg_assert,
                    ))
                })
                .collect();

            let profiles = handles.into_iter().flat_map(|h| h.join().expect("benchmark thread panicked")).collect();
            MultiTxMultiQueryProfile {
                name: name.clone(),
                profiles,
                run_descriptor: run_descriptor.clone(),
                total_wall_time: very_beginning.elapsed(),
                tx_type,
            }
        })
    }

    fn new_runner_thread<TxnType: UnifiedTransactionView>(
        database: Arc<Database<WALClient>>,
        producer: Arc<WorkloadInputBatches>,
        query: Arc<String>,
        run_descriptor: RunDescriptor,
        dbg_assert: Option<DbgAssertFn>,
    ) -> impl FnOnce() -> Vec<MultiQueryTxProfile> {
        let overestimate_txns_per_thread: usize =
            ((1.5 * run_descriptor.total_txns as f64 / run_descriptor.parallelism as f64).ceil() as usize).max(2);
        let local_profiles = Vec::with_capacity(overestimate_txns_per_thread);
        let query: Arc<String> = query.clone();
        move || {
            let mut local_profiles = local_profiles;
            while producer.has_remaining() {
                let mut query_profiles = Vec::with_capacity(run_descriptor.n_queries_per_tx);
                let mut tx = TxnType::open(database.clone(), transaction_options_with_profiling()).unwrap();
                let start = Instant::now();
                for _ in 0..run_descriptor.n_queries_per_tx {
                    let Some(given_rows) = producer.take_next_batch() else {
                        break;
                    };
                    let (query_result, tx_returned) = tx.query::<CountResults>(query.as_str(), Some(given_rows), true);
                    tx = tx_returned;
                    let QueryAnswer { profile: query_profile, answer: rows } = query_result.unwrap();
                    if let Some(dbg_assert) = &dbg_assert {
                        dbg_assert(rows);
                    };
                    query_profiles.push(query_profile);
                }
                if !query_profiles.is_empty() {
                    let tx_profile = tx.commit().unwrap();
                    local_profiles.push(MultiQueryTxProfile {
                        tx_profile,
                        query_profiles,
                        time_elapsed: start.elapsed(),
                    });
                }
            }
            local_profiles
        }
    }
}

/// Pre-generated batches consumed work-stealing style.
/// Each slot is claimed by exactly one thread via fetch_add, so no locking needed.
pub struct WorkloadInputBatches {
    batches: Vec<UnsafeCell<Option<GivenRowsSimple>>>,
    next_index: AtomicUsize,
}

// Safety: each index is claimed by exactly one thread (fetch_add ensures uniqueness).
unsafe impl Sync for WorkloadInputBatches {}

impl WorkloadInputBatches {
    fn build(query: &QueryDescriptor, run: &RunDescriptor) -> Arc<Self> {
        let mut rng = RandomDataGen::new();
        let batches = (0..(run.total_txns * run.n_queries_per_tx))
            .map(|_| {
                let given_rows_opt = query.produce_row.map(|produce| {
                    let rows = (0..run.n_rows_per_query).map(|_| produce(&mut rng)).collect();
                    GivenRowsSimple { variables: query.variables.clone(), rows }
                });
                UnsafeCell::new(given_rows_opt)
            })
            .collect();
        Arc::new(Self { batches, next_index: AtomicUsize::new(0) })
    }

    fn has_remaining(&self) -> bool {
        self.next_index.load(Ordering::Relaxed) < self.batches.len()
    }

    fn take_next_batch(&self) -> Option<GivenRowsSimple> {
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        if idx >= self.batches.len() {
            return None;
        }
        // Safety: idx is unique — no other thread will ever access this slot.
        unsafe { (*self.batches[idx].get()).take() }
    }
}
