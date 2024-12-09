/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::format, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

#[derive(Debug)]
pub struct DiagnosticsManager {
    // TODO: Rename? Can't actually name it a service
    diagnostics: Diagnostics,
    reporter: Reporter,
    monitoring_server: MonitoringServer,
}

impl DiagnosticsManager {
    pub fn new() -> Self {
        Self { diagnostics: Diagnostcs::new(), reporter: Reporter::new(), monitoring_server: MonitoringServer::new() }
    }

    // pub fn synchronize_database_metrics(metrics: DatabaseMetrics);

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
