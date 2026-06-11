/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use concurrency::TokioTaskSpawner;
use resource::constants::database::INTERNAL_DATABASE_PREFIX;

use crate::{
    Diagnostics, MonitoringSection,
    metrics::{ActionKind, ClientEndpoint, DatabaseMetricsSnapshot, LoadKind, QueryType, TransactionOutcome},
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
        let deployment_id = diagnostics.typedb().server_properties.deployment_id().to_owned();
        let data_directory = diagnostics.typedb().server_metrics.data_directory().clone();
        let is_reporting_enabled = diagnostics.typedb().server_properties.is_reporting_enabled();
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

    pub fn new_disabled() -> Self {
        let diagnostics = Diagnostics::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            std::path::PathBuf::new(),
            false,
            false,
        );
        Self { diagnostics: Arc::new(diagnostics), reporter: None, monitoring_server: None }
    }

    pub fn metrics_enabled(&self) -> bool {
        self.diagnostics.metrics_enabled()
    }

    pub fn register_monitoring_extension(&self, source: Arc<dyn MonitoringSection>) {
        self.diagnostics.register_monitoring_extension(source);
    }

    pub fn has_monitoring_extension(&self, name: &str) -> bool {
        self.diagnostics.has_monitoring_extension(name)
    }

    diagnostics_method! {
        pub fn submit_database_metrics(&self, snapshots: HashMap<Arc<str>, DatabaseMetricsSnapshot>);
        pub fn submit_error(&self, client: ClientEndpoint, database_name: Option<&str>, error_code: String);
        pub fn submit_action_success(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind);
        pub fn submit_action_fail(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind);
        pub fn increment_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind);
        pub fn decrement_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind);

        pub fn observe_query_duration(&self, database_name: &str, kind: QueryType, duration: std::time::Duration);
        pub fn observe_transaction_duration(&self, database_name: &str, kind: LoadKind, duration: std::time::Duration);
        pub fn observe_queries_per_transaction(&self, database_name: &str, queries: u64);
        pub fn record_transaction_outcome(&self, database_name: &str, kind: LoadKind, outcome: TransactionOutcome);
    }

    pub fn wal_metrics(&self, database_name: &str, is_internal_database: bool) -> crate::metrics::FsyncMetrics {
        if !self.metrics_enabled() || is_internal_database {
            return crate::metrics::FsyncMetrics::disabled();
        }
        self.diagnostics.wal_metrics(database_name)
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

pub fn is_diagnostics_needed(database_name: Option<&str>) -> bool {
    match database_name {
        Some(database_name) => !database_name.starts_with(INTERNAL_DATABASE_PREFIX),
        None => true,
    }
}
