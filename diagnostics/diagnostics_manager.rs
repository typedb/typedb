/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, hash::Hash, sync::Arc};

use concurrency::TokioTaskSpawner;
use resource::constants::database::INTERNAL_DATABASE_PREFIX;

use crate::{
    Diagnostics,
    metrics::{ActionKind, ClientEndpoint, DatabaseMetrics, LoadKind, QueryType, TransactionOutcome},
    monitoring_server::MonitoringServer,
    reporter::Reporter,
};

macro_rules! diagnostics_method {
    ($(
        pub fn $fn_name:ident(&self, $($arg_name:ident: $arg_type:ty),* $(,)?) ;
    )*) => {
        $(
            pub fn $fn_name(&self, $($arg_name: $arg_type),*) {
                self.diagnostics.$fn_name($($arg_name),*)
            }
        )*
    };
}

#[derive(Debug)]
pub struct DiagnosticsManager {
    diagnostics: Arc<Diagnostics>,
    reporter: Option<Reporter>,
    monitoring_server: Option<MonitoringServer>,
}

impl DiagnosticsManager {
    pub fn new(
        diagnostics: Diagnostics,
        monitoring_port: u16,
        is_monitoring_enabled: bool,
        is_development_mode: bool,
        background_tasks: TokioTaskSpawner,
    ) -> Self {
        let deployment_id = diagnostics.server_properties.deployment_id().to_owned();
        let data_directory = diagnostics.server_metrics.data_directory().clone();
        let is_reporting_enabled = diagnostics.server_properties.is_reporting_enabled();
        let diagnostics = Arc::new(diagnostics);

        let reporter = if is_development_mode {
            None
        } else {
            Some(Reporter::new(
                deployment_id,
                diagnostics.clone(),
                data_directory,
                is_reporting_enabled,
                background_tasks,
            ))
        };

        let monitoring_server = if is_monitoring_enabled {
            Some(MonitoringServer::new(diagnostics.clone(), monitoring_port))
        } else {
            None
        };

        Self { diagnostics, reporter, monitoring_server }
    }

    /// Construct a no-op manager for tests and benches. No reporter or monitoring
    /// server is created; `is_collection_needed` is false so every observation
    /// method short-circuits at zero cost (per SR5). Suitable for callers of
    /// `DatabaseManager::new` that don't care about diagnostics.
    pub fn new_test() -> Self {
        let diagnostics = Diagnostics::new(
            String::new(),             // deployment_id
            String::new(),             // server_id
            String::new(),             // distribution
            String::new(),             // version
            std::path::PathBuf::new(), // data_directory
            false,                     // is_reporting_enabled
            false,                     // is_collection_needed
        );
        Self { diagnostics: Arc::new(diagnostics), reporter: None, monitoring_server: None }
    }

    /// Whether anyone will read the collected metrics; mirrors the flag passed to
    /// `Diagnostics::new`. Callers that own a metric-source adapter can use this to
    /// substitute a no-op implementation and skip per-event work entirely.
    pub fn is_collection_needed(&self) -> bool {
        self.diagnostics.is_collection_needed()
    }

    diagnostics_method! {
        pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>);
        pub fn submit_error(&self, client: ClientEndpoint, database_name: Option<impl AsRef<str> + Hash>, error_code: String);
        pub fn submit_action_success(&self, client: ClientEndpoint, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind);
        pub fn submit_action_fail(&self, client: ClientEndpoint, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind);
        pub fn increment_load_count(&self, client: ClientEndpoint, database_name: impl AsRef<str> + Hash, load_kind: LoadKind);
        pub fn decrement_load_count(&self, client: ClientEndpoint, database_name: impl AsRef<str> + Hash, load_kind: LoadKind);

        pub fn observe_query_duration(&self, database_name: impl AsRef<str> + Hash, kind: QueryType, duration: std::time::Duration);
        pub fn observe_transaction_duration(&self, database_name: impl AsRef<str> + Hash, kind: LoadKind, duration: std::time::Duration);
        pub fn observe_queries_per_transaction(&self, database_name: impl AsRef<str> + Hash, queries: u64);
        pub fn record_transaction_outcome(&self, database_name: impl AsRef<str> + Hash, kind: LoadKind, outcome: TransactionOutcome);
    }

    pub fn wal_metrics_handles(
        &self,
        database_name: impl AsRef<str> + Hash,
    ) -> (std::sync::Arc<crate::metrics::HistogramMetrics>, std::sync::Arc<std::sync::atomic::AtomicU64>) {
        self.diagnostics.wal_metrics_handles(database_name)
    }

    pub async fn may_start_reporting(&self) {
        if let Some(reporter) = &self.reporter {
            reporter.may_start().await;
        }
    }

    pub async fn may_start_monitoring(&self) {
        if let Some(server) = &self.monitoring_server {
            server.start_serving().await;
        }
    }
}

pub fn is_diagnostics_needed(database_name: Option<impl AsRef<str> + Hash>) -> bool {
    // TODO: Would be good to reuse DatabaseManager's is_user_database() instead
    match database_name {
        Some(database_name) => !database_name.as_ref().starts_with(INTERNAL_DATABASE_PREFIX),
        None => true,
    }
}
