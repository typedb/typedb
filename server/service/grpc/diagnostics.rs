/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{future::Future, hash::Hash, sync::Arc};

use diagnostics::{
    diagnostics_manager::{is_diagnostics_needed, DiagnosticsManager},
    metrics::{ActionKind, ClientEndpoint},
};
use tonic::Status;
use tonic_types::StatusExt;

pub(crate) fn run_with_diagnostics<F, T>(
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

pub(crate) async fn run_with_diagnostics_async<F, Fut, T>(
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
        Ok(_) => diagnostics_manager.submit_action_success(ClientEndpoint::Grpc, database_name, action_kind),
        Err(status) => {
            diagnostics_manager.submit_action_fail(ClientEndpoint::Grpc, database_name.as_ref(), action_kind);
            if let Some(error_code) = get_status_error_code(status) {
                diagnostics_manager.submit_error(ClientEndpoint::Grpc, database_name, error_code.clone());
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
