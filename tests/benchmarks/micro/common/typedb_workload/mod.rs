/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, time::Duration};

use benchmark::{WarmupFn, WorkLoad};
use datagen::RandomDataGen;
use query::given_rows::GivenRowEntry;
use resource::profile::{QueryProfile, TransactionProfile};

pub mod benchmark;
pub mod datagen;
pub mod reports;
pub mod run_configs;

#[derive(Clone)]
pub struct QueryDescriptor {
    pub name: String,
    pub query: String,
    pub variables: Vec<String>,
    pub produce_row: Option<fn(&mut RandomDataGen) -> Vec<GivenRowEntry>>,
}

impl QueryDescriptor {
    pub fn for_warmup(&self) -> Option<WarmupFn> {
        let warmup_workload = self.create_workload(RunDescriptor::WARMUP.clone());
        let runner = warmup_workload.runner("_warmup");
        let prepare_fn = warmup_workload.prepare_fn();
        Some(Box::new(move |db| {
            let db_clone = db.clone();
            runner(db, prepare_fn(db_clone));
        }))
    }

    pub fn create_workload(&self, run_descriptor: RunDescriptor) -> WorkLoad {
        WorkLoad { query_descriptor: self.clone(), run_descriptor }
    }
}

#[derive(Clone)]
pub struct RunDescriptor {
    pub parallelism: usize,
    pub total_txns: usize,
    pub n_queries_per_tx: usize,
    pub n_rows_per_query: usize,
}

impl RunDescriptor {
    const WARMUP: RunDescriptor =
        RunDescriptor { parallelism: 1, total_txns: 10, n_queries_per_tx: 1, n_rows_per_query: 10 };

    pub fn total_rows(&self) -> usize {
        self.total_txns * self.n_queries_per_tx * self.n_rows_per_query
    }
}

pub fn transaction_options_with_profiling() -> options::TransactionOptions {
    let mut tx_options = options::TransactionOptions::default();
    tx_options.enable_profiling = Some(true);
    tx_options
}

pub struct TxQueryProfile {
    pub tx_profile: Option<TransactionProfile>,
    pub query_profile: Arc<QueryProfile>,
}

pub struct MultiQueryTxProfile {
    pub tx_profile: TransactionProfile,
    pub query_profiles: Vec<Arc<QueryProfile>>,
    pub time_elapsed: Duration,
}

pub struct MultiTxMultiQueryProfile {
    pub name: String,
    pub profiles: Vec<MultiQueryTxProfile>,
    pub run_descriptor: RunDescriptor,
    pub total_wall_time: Duration,
}
