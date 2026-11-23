/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    future::Future,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Instant,
};

use axum::extract::ConnectInfo;
use diagnostics::{
    diagnostics_manager::is_diagnostics_needed,
    metrics::{ActionKind, ClientEndpoint},
};
use error::TypeDBError;
use http::Extensions;
use query::error::QueryError;
use tonic::{
    Status,
    transport::server::{TcpConnectInfo, TlsConnectInfo},
};
use tonic_types::StatusExt;
use uuid::Uuid;

use crate::{
    audit::{AuditAdmission, AuditEndpoint, AuditError, AuditEvent, AuditOutcome, AuditReason, AuditStage},
    authentication::Accessor,
    service::{QueryType, http::error::HttpServiceError},
    state::ServerState,
};

#[derive(Debug, Clone)]
pub(crate) struct ActionOrigin {
    endpoint: AuditEndpoint,
    peer_address: Option<SocketAddr>,
    accessor: Option<Arc<str>>,
}

impl ActionOrigin {
    pub(crate) fn grpc(extensions: &Extensions) -> Self {
        let peer_address = match extensions.get::<TcpConnectInfo>() {
            Some(connect_info) => connect_info.remote_addr(),
            None => extensions
                .get::<TlsConnectInfo<TcpConnectInfo>>()
                .and_then(|connect_info| connect_info.get_ref().remote_addr()),
        };
        Self { endpoint: AuditEndpoint::Grpc, peer_address, accessor: Self::accessor(extensions) }
    }

    pub(crate) fn http(extensions: &Extensions) -> Self {
        let peer_address = extensions.get::<ConnectInfo<SocketAddr>>().map(|ConnectInfo(address)| *address);
        Self { endpoint: AuditEndpoint::Http, peer_address, accessor: Self::accessor(extensions) }
    }

    pub(crate) fn admin() -> Self {
        Self { endpoint: AuditEndpoint::Admin, peer_address: None, accessor: None }
    }

    fn accessor(extensions: &Extensions) -> Option<Arc<str>> {
        extensions.get::<Accessor>().map(|accessor| Arc::from(accessor.as_str()))
    }

    fn client_endpoint(&self) -> Option<ClientEndpoint> {
        match self.endpoint {
            AuditEndpoint::Grpc => Some(ClientEndpoint::Grpc),
            AuditEndpoint::Http => Some(ClientEndpoint::Http),
            AuditEndpoint::Admin => None,
        }
    }
}

pub(crate) trait ActionFailure {
    fn code(&self) -> Option<String>;

    fn message(&self) -> String;
}

impl ActionFailure for Status {
    fn code(&self) -> Option<String> {
        let details = self.check_error_details().ok()?;
        details.error_info().map(|info| info.reason.clone())
    }

    fn message(&self) -> String {
        let details = self.check_error_details().ok();
        match details.as_ref().and_then(|details| details.debug_info()) {
            Some(debug_info) if !debug_info.stack_entries.is_empty() => debug_info.stack_entries.join("\n"),
            Some(_) | None => Status::message(self).to_owned(),
        }
    }
}

impl ActionFailure for typedb_protocol::Error {
    fn code(&self) -> Option<String> {
        Some(self.error_code.clone())
    }

    fn message(&self) -> String {
        self.stack_trace.join("\n")
    }
}

impl ActionFailure for HttpServiceError {
    fn code(&self) -> Option<String> {
        Some(self.root_source_typedb_error().code().to_owned())
    }

    fn message(&self) -> String {
        self.stack_trace().join("\n")
    }
}

impl ActionFailure for QueryError {
    fn code(&self) -> Option<String> {
        Some(self.root_source_typedb_error().code().to_owned())
    }

    fn message(&self) -> String {
        self.stack_trace().join("\n")
    }
}

#[derive(Debug)]
pub(crate) struct Action {
    server_state: Arc<ServerState>,
    details: ActionDetails,
    started_at: Instant,
    is_finished: bool,
}

#[derive(Debug, Clone)]
struct ActionDetails {
    kind: ActionKind,
    origin: ActionOrigin,
    operation_id: Uuid,
    target_username: Option<String>,
    database_name: Option<Arc<str>>,
    transaction_id: Option<Uuid>,
    query: Option<Arc<str>>,
    forced_by: Option<AuditReason>,
    start: Arc<OnceLock<ActionStartState>>,
}

#[derive(Debug, Clone, Copy)]
enum ActionStartState {
    Started { query_type: Option<QueryType> },
    EndedBeforeStarting,
}

impl ActionDetails {
    async fn report_start_unless_ended(&self, server_state: &ServerState, query_type: Option<QueryType>) -> bool {
        let mut admission = AuditAdmission::immediate();
        let state = *self.start.get_or_init(|| {
            let auditor = server_state.auditor();
            if auditor.is_enabled() {
                let stage = AuditStage::Started { query: self.query.as_deref() };
                admission = auditor.record(self.audit_event(query_type, stage));
            }
            ActionStartState::Started { query_type }
        });
        admission.await;
        matches!(state, ActionStartState::Started { .. })
    }

    fn start_state_at_end(&self) -> ActionStartState {
        *self.start.get_or_init(|| ActionStartState::EndedBeforeStarting)
    }

    fn audit_event<'a>(&'a self, query_type: Option<QueryType>, stage: AuditStage<'a>) -> AuditEvent<'a> {
        AuditEvent {
            action_kind: self.kind,
            endpoint: self.origin.endpoint,
            peer_address: self.origin.peer_address,
            accessor: self.origin.accessor.as_deref(),
            target_username: self.target_username.as_deref(),
            database_name: self.database_name.as_deref(),
            transaction_id: self.transaction_id,
            operation_id: self.operation_id,
            query_type,
            forced_by: self.forced_by,
            stage,
        }
    }
}

impl Action {
    pub(crate) fn new(server_state: Arc<ServerState>, kind: ActionKind, origin: ActionOrigin) -> Self {
        let details = ActionDetails {
            kind,
            origin,
            operation_id: Uuid::new_v4(),
            target_username: None,
            database_name: None,
            transaction_id: None,
            query: None,
            forced_by: None,
            start: Arc::new(OnceLock::new()),
        };
        Self { server_state, details, started_at: Instant::now(), is_finished: false }
    }

    pub(crate) fn with_target_username(mut self, username: impl Into<String>) -> Self {
        self.details.target_username = Some(username.into());
        self
    }

    pub(crate) fn with_database(mut self, database_name: impl Into<Arc<str>>) -> Self {
        self.set_database(database_name);
        self
    }

    pub(crate) fn with_transaction_id(mut self, transaction_id: Uuid) -> Self {
        self.set_transaction_id(transaction_id);
        self
    }

    pub(crate) fn forced_by(mut self, reason: AuditReason) -> Self {
        self.details.forced_by = Some(reason);
        self
    }

    pub(crate) fn with_query(mut self, query: &str) -> Self {
        if self.server_state.auditor().is_enabled() {
            self.details.query = Some(Arc::from(query));
        }
        self
    }

    pub(crate) fn set_database(&mut self, database_name: impl Into<Arc<str>>) {
        self.details.database_name = Some(database_name.into());
    }

    pub(crate) fn set_transaction_id(&mut self, transaction_id: Uuid) {
        self.details.transaction_id = Some(transaction_id);
    }

    pub(crate) async fn report_start(&self) {
        self.details.report_start_unless_ended(&self.server_state, None).await;
    }

    pub(crate) async fn report_query_start(&self, query_type: QueryType) {
        self.details.report_start_unless_ended(&self.server_state, Some(query_type)).await;
    }

    pub(crate) fn hand_over_start(&self) -> Option<ActionStart> {
        self.server_state
            .auditor()
            .is_enabled()
            .then(|| ActionStart { server_state: self.server_state.clone(), details: self.details.clone() })
    }

    pub(crate) async fn run<F, Fut, T, E>(self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: ActionFailure,
    {
        self.report_start().await;
        let result = f().await;
        self.finish(&result);
        result
    }

    pub(crate) fn finish<T, E: ActionFailure>(self, result: &Result<T, E>) {
        match result {
            Ok(_) => self.succeed(),
            Err(error) => self.fail(error),
        }
    }

    pub(crate) fn succeed(mut self) {
        self.submit(ActionOutcome::Success);
    }

    pub(crate) fn fail(mut self, error: &impl ActionFailure) {
        self.submit(ActionOutcome::Failure(error));
    }

    fn submit(&mut self, outcome: ActionOutcome<'_>) {
        self.is_finished = true;
        self.submit_diagnostics(&outcome);
        self.submit_audit(&outcome);
    }

    fn submit_diagnostics(&self, outcome: &ActionOutcome<'_>) {
        let Some(client) = self.details.origin.client_endpoint() else {
            return;
        };
        let database_name = self.details.database_name.as_deref();
        if self.details.forced_by.is_some() || !is_diagnostics_needed(database_name) {
            return;
        }

        let diagnostics_manager = self.server_state.diagnostics_manager();
        let kind = self.details.kind;
        match outcome {
            ActionOutcome::Success => diagnostics_manager.submit_action_success(client, database_name, kind),
            ActionOutcome::Failure(error) => {
                diagnostics_manager.submit_action_fail(client, database_name, kind);
                if let Some(code) = error.code() {
                    diagnostics_manager.submit_error(client, database_name, code);
                }
            }
            ActionOutcome::Abandoned => diagnostics_manager.submit_action_fail(client, database_name, kind),
        }
    }

    fn submit_audit(&self, outcome: &ActionOutcome<'_>) {
        let auditor = self.server_state.auditor();
        if !auditor.is_enabled() {
            return;
        }
        let start = self.details.start_state_at_end();
        let (code, message);
        let outcome = match outcome {
            ActionOutcome::Success => AuditOutcome::Success,
            ActionOutcome::Failure(error) => {
                (code, message) = (error.code(), error.message());
                AuditOutcome::Failure(AuditError { code: code.as_deref(), message: &message })
            }
            ActionOutcome::Abandoned => AuditOutcome::Abandoned,
        };
        let duration = self.started_at.elapsed();
        let (query_type, unreported_query) = match start {
            ActionStartState::Started { query_type } => (query_type, None),
            ActionStartState::EndedBeforeStarting => (None, self.details.query.as_deref()),
        };
        let stage = AuditStage::Ended { duration, outcome, unreported_query };
        auditor.record(self.details.audit_event(query_type, stage));
    }
}

impl Drop for Action {
    fn drop(&mut self) {
        if !self.is_finished {
            self.submit(ActionOutcome::Abandoned);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActionStart {
    server_state: Arc<ServerState>,
    details: ActionDetails,
}

impl ActionStart {
    pub(crate) async fn report_query_unless_ended(self, query_type: QueryType) -> bool {
        self.details.report_start_unless_ended(&self.server_state, Some(query_type)).await
    }
}

enum ActionOutcome<'a> {
    Success,
    Failure(&'a dyn ActionFailure),
    Abandoned,
}
