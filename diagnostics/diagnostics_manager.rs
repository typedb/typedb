/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt::format, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc, time::Instant};

use error::TypeDBError;

use crate::{
    metrics::{ActionKind, DatabaseMetrics},
    Diagnostics,
};

#[derive(Debug)]
pub struct DiagnosticsManager {
    diagnostics: Diagnostics,
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
        reporting_enabled: bool,
    ) -> Self {
        Self {
            diagnostics: Diagnostics::new(
                deployment_id,
                server_id,
                distribution,
                version,
                data_directory,
                reporting_enabled,
            ),
            reporter: Reporter::new(),
            monitoring_server: MonitoringServer::new(),
        }
    }

    pub fn synchronize_database_metrics(&self, metrics: HashSet<DatabaseMetrics>) {
        todo!()
    }

    pub fn submit_error(database_name: Option<&str>, error: &impl TypeDBError) {
        todo!()
        // error.code();
    }

    pub fn submit_action_success(database_name: Option<&str>, action_kind: ActionKind) {
        todo!()
    }

    pub fn submit_action_fail(database_name: Option<&str>, action_kind: ActionKind) {
        todo!()
    }

    pub fn increment_current_count(database_name: Option<&str>, connection_: ActionKind) {
        todo!()
    }

    pub fn decrement_current_count(database_name: Option<&str>, connection_: ActionKind) {
        todo!()
    }
}
