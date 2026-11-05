use std::{sync::Arc, time::Duration};

use resource::profile::{QueryProfile, TransactionProfile};

use crate::typedb_workload::{RunDescriptor, TxType};

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
    pub tx_type: TxType,
}
