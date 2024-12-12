/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    error::Error,
    fmt::format,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use error::TypeDBError;
use resource::constants::diagnostics::REPORTING_URI;
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
    reporter: Reporter,
    monitoring_server: MonitoringServer,
}

impl DiagnosticsManager {
    pub fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        monitoring_port: u16,
        is_monitoring_enabled: bool,
        is_reporting_enabled: bool,
    ) -> Self {
        let diagnostics = Arc::new(Diagnostics::new(
            deployment_id.clone(),
            server_id,
            distribution,
            version,
            data_directory.clone(),
            is_reporting_enabled,
        ));
        let reporter =
            Reporter::new(deployment_id, diagnostics.clone(), REPORTING_URI, data_directory, is_reporting_enabled);
        let monitoring_server = MonitoringServer::new(diagnostics.clone(), monitoring_port, is_monitoring_enabled);

        Self { diagnostics, reporter, monitoring_server }
    }

    diagnostics_method! {
        pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>);
        pub fn submit_error(&self, database_name: Option<&str>, error_code: String);
        pub fn submit_action_success(&self, database_name: Option<&str>, action_kind: ActionKind);
        pub fn submit_action_fail(&self, database_name: Option<&str>, action_kind: ActionKind);
        pub fn increment_load_count(&self, database_name: &str, connection_: LoadKind);
        pub fn decrement_load_count(&self, database_name: &str, connection_: LoadKind);
    }

    pub fn may_start_reporting(&self) {
        self.reporter.may_start()
    }

    pub fn may_start_monitoring(&self) {
        self.monitoring_server.may_start()
    }
}

pub struct DiagnosticsResultTracker<'d> {
    diagnostics_manager: &'d DiagnosticsManager,
    database_name: Option<&'d str>,
    action_kind: ActionKind,
    error_code: Option<String>,
}

impl<'d> DiagnosticsResultTracker<'d> {
    pub fn without_database(diagnostics_manager: &'d DiagnosticsManager, action_kind: ActionKind) -> Self {
        DiagnosticsResultTracker { diagnostics_manager, database_name: None, action_kind, error_code: None }
    }

    pub fn with_database(
        diagnostics_manager: &'d DiagnosticsManager,
        database_name: &'d str,
        action_kind: ActionKind,
    ) -> Self {
        DiagnosticsResultTracker {
            diagnostics_manager,
            database_name: Some(database_name),
            action_kind,
            error_code: None,
        }
    }

    pub fn set_error_code(&mut self, error_code: String) {
        self.error_code = Some(error_code);
    }
}

impl<'d> Drop for DiagnosticsResultTracker<'d> {
    fn drop(&mut self) {
        match &self.error_code {
            Some(error_code) => {
                self.diagnostics_manager.submit_action_fail(self.database_name, self.action_kind);
                self.diagnostics_manager.submit_error(self.database_name, error_code.clone());
            }
            None => self.diagnostics_manager.submit_action_success(self.database_name, self.action_kind),
        }
    }
}

pub async fn run_with_diagnostics<F, T>(
    diagnostics_manager: &DiagnosticsManager,
    action_kind: ActionKind,
    f: F,
) -> Result<T, Status>
where
    F: FnOnce() -> Result<T, Status>,
{
    run_and_track(DiagnosticsResultTracker::without_database(diagnostics_manager, action_kind), f).await
}

pub async fn run_with_diagnostics_for_database<F, T>(
    diagnostics_manager: &DiagnosticsManager,
    database_name: &str,
    action_kind: ActionKind,
    f: F,
) -> Result<T, Status>
where
    F: FnOnce() -> Result<T, Status>,
{
    run_and_track(DiagnosticsResultTracker::with_database(diagnostics_manager, database_name, action_kind), f).await
}

async fn run_and_track<F, T>(mut tracker: DiagnosticsResultTracker<'_>, f: F) -> Result<T, Status>
where
    F: FnOnce() -> Result<T, Status>,
{
    let result = f();
    if let Err(ref status) = result {
        tracker.set_error_code(get_status_error_code(status));
    }
    result
}

fn get_status_error_code(status: &Status) -> String {
    if let Ok(details) = status.check_error_details() {
        if let Some(error_info) = details.error_info() {
            return error_info.reason.clone();
        }
    }
    "UKNOWN".to_owned() // TODO: What to do with error codes?
}
