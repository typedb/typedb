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

use crate::service::http::error::HttpServiceError;

pub(crate) fn run_with_diagnostics<F, T>(
    diagnostics_manager: &DiagnosticsManager,
    database_name: Option<impl AsRef<str> + Hash>,
    action_kind: ActionKind,
    f: F,
) -> Result<T, HttpServiceError>
where
    F: FnOnce() -> Result<T, HttpServiceError>,
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
) -> Result<T, HttpServiceError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, HttpServiceError>> + Send,
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
    result: &Result<T, HttpServiceError>,
) {
    if !is_diagnostics_needed(database_name.as_ref()) {
        return;
    }

    match result {
        Ok(_) => diagnostics_manager.submit_action_success(ClientEndpoint::Http, database_name, action_kind),
        Err(error) => {
            diagnostics_manager.submit_action_fail(ClientEndpoint::Http, database_name.as_ref(), action_kind);
            diagnostics_manager.submit_error(ClientEndpoint::Http, database_name, error.source().code().to_string());
        }
    }
}
