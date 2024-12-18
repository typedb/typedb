/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, future::Future, hash::Hash, sync::Arc};

use resource::constants::{database::INTERNAL_DATABASE_PREFIX, diagnostics::REPORTING_URI};
use tonic::Status;
use tonic_types::StatusExt;

use crate::{
    metrics::{ActionKind, DatabaseMetrics, LoadKind},
    monitoring_server::MonitoringServer,
    reporter::Reporter,
    Diagnostics,
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
    ) -> Self {
        let deployment_id = diagnostics.server_properties.deployment_id().clone().to_owned();
        let data_directory = diagnostics.server_metrics.data_directory().clone();
        let is_reporting_enabled = diagnostics.server_properties.is_reporting_enabled();
        let diagnostics = Arc::new(diagnostics);

        let reporter = if is_development_mode {
            None
        } else {
            Some(Reporter::new(deployment_id, diagnostics.clone(), REPORTING_URI, data_directory, is_reporting_enabled))
        };

        let monitoring_server = if is_monitoring_enabled {
            Some(MonitoringServer::new(diagnostics.clone(), monitoring_port))
        } else {
            None
        };

        Self { diagnostics, reporter, monitoring_server }
    }

    diagnostics_method! {
        pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>);
        pub fn submit_error(&self, database_name: Option<impl AsRef<str> + Hash>, error_code: String);
        pub fn submit_action_success(&self, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind);
        pub fn submit_action_fail(&self, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind);
        pub fn increment_load_count(&self, database_name: &str, connection_: LoadKind);
        pub fn decrement_load_count(&self, database_name: &str, connection_: LoadKind);
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

pub fn run_with_diagnostics<F, T>(
    diagnostics_manager: &DiagnosticsManager,
    database_name: Option<impl AsRef<str> + Hash>,
    action_kind: ActionKind,
    f: F,
) -> Result<T, Status>
where
    F: FnOnce() -> Result<T, Status>,
{
    let result = f();
    submit_result_metrics(diagnostics_manager, database_name, action_kind, &result);
    result
}

pub async fn run_with_diagnostics_async<F, Fut, T>(
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Option<impl AsRef<str> + Hash>,
    action_kind: ActionKind,
    f: F,
) -> Result<T, Status>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Status>> + Send,
    T: Send,
{
    let result = f().await;
    submit_result_metrics(&diagnostics_manager, database_name, action_kind, &result);
    result
}

fn submit_result_metrics<T>(
    diagnostics_manager: &DiagnosticsManager,
    database_name: Option<impl AsRef<str> + Hash>,
    action_kind: ActionKind,
    result: &Result<T, Status>,
) {
    if !is_diagnostics_needed(database_name.as_ref()) {
        return;
    }

    match result {
        Ok(_) => diagnostics_manager.submit_action_success(database_name, action_kind),
        Err(status) => {
            diagnostics_manager.submit_action_fail(database_name.as_ref(), action_kind);
            if let Some(error_code) = get_status_error_code(status) {
                diagnostics_manager.submit_error(database_name, error_code.clone());
            }
        }
    }
}

fn get_status_error_code(status: &Status) -> Option<String> {
    if let Ok(details) = status.check_error_details() {
        if let Some(error_info) = details.error_info() {
            return Some(error_info.reason.clone());
        }
    }
    None // status.code() can return too long string descriptions from external libraries
}

fn is_diagnostics_needed(database_name: Option<impl AsRef<str> + Hash>) -> bool {
    // TODO: Would be good to reuse DatabaseManager's is_user_database() instead
    match database_name {
        Some(database_name) => !database_name.as_ref().starts_with(INTERNAL_DATABASE_PREFIX),
        None => true,
    }
}
