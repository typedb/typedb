/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::format,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use error::TypeDBError;
use resource::constants::diagnostics::REPORTING_URI;

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
        reporting_enabled: bool,
    ) -> Self {
        let diagnostics = Arc::new(Diagnostics::new(
            deployment_id.clone(),
            server_id,
            distribution,
            version,
            data_directory.clone(),
            reporting_enabled,
        ));
        let reporter =
            Reporter::new(deployment_id, diagnostics.clone(), REPORTING_URI, data_directory, reporting_enabled);
        let monitoring_server = MonitoringServer::new(diagnostics.clone(), monitoring_port);

        Self { diagnostics, reporter, monitoring_server }
    }

    diagnostics_method! {
        pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>);
        pub fn submit_error(&self, database_name: Option<&str>, error: &impl TypeDBError);
        pub fn submit_action_success(&self, database_name: Option<&str>, action_kind: ActionKind);
        pub fn submit_action_fail(&self, database_name: Option<&str>, action_kind: ActionKind);
        pub fn increment_load_count(&self, database_name: Option<&str>, connection_: LoadKind);
        pub fn decrement_load_count(&self, database_name: Option<&str>, connection_: LoadKind);
    }
}
