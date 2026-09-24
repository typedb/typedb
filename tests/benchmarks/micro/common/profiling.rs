/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Duration};

use resource::profile::{QueryProfile, TransactionProfile};

use crate::benchmark::RunDescriptor;

pub fn transaction_options_with_profiling() -> options::TransactionOptions {
    let mut tx_options = options::TransactionOptions::default();
    tx_options.tmp_enable_profiling = Some(true);
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
    pub name: &'static str,
    pub profiles: Vec<MultiQueryTxProfile>,
    pub run_descriptor: RunDescriptor,
    pub total_wall_time: Duration,
}
