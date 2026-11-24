/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, future::Future, net::SocketAddr, pin::Pin, time::Duration};

use diagnostics::metrics::ActionKind;
use uuid::Uuid;

use crate::service::QueryType;

pub trait Auditor: Debug + Send + Sync {
    fn is_enabled(&self) -> bool;

    fn record(&self, event: AuditEvent<'_>) -> AuditAdmission;
}

pub struct AuditAdmission(Option<Pin<Box<dyn Future<Output = ()> + Send>>>);

impl AuditAdmission {
    pub fn immediate() -> Self {
        Self(None)
    }

    pub fn after(wait: impl Future<Output = ()> + Send + 'static) -> Self {
        Self(Some(Box::pin(wait)))
    }

    pub async fn wait(self) {
        if let Some(wait) = self.0 {
            wait.await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuditEndpoint {
    Grpc,
    Http,
    Admin,
}

#[derive(Debug, Clone)]
pub struct AuditEvent<'a> {
    pub action_kind: ActionKind,
    pub endpoint: AuditEndpoint,
    pub peer_address: Option<SocketAddr>,
    pub accessor: Option<&'a str>,
    pub target_username: Option<&'a str>,
    pub database_name: Option<&'a str>,
    pub transaction_id: Option<Uuid>,
    pub operation_id: Uuid,
    pub query_type: Option<QueryType>,
    pub forced_by: Option<ForcedAuditReason>,
    pub stage: AuditStage<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForcedAuditReason {
    ConnectionEnd,
    RequestFailure,
    Timeout,
    Shutdown,
    ServerStateChange,
}

#[derive(Debug, Clone, Copy)]
pub enum AuditStage<'a> {
    Started { query: Option<&'a str> },
    Ended { duration: Duration, outcome: AuditOutcome<'a>, unreported_query: Option<&'a str> },
}

#[derive(Debug, Clone, Copy)]
pub enum AuditOutcome<'a> {
    Success,
    Failure(AuditFailure<'a>),
    Abandoned,
}

#[derive(Debug, Clone, Copy)]
pub struct AuditFailure<'a> {
    pub code: Option<&'a str>,
    pub message: &'a str,
}

#[derive(Debug)]
pub struct NoopAuditor;

impl Auditor for NoopAuditor {
    fn is_enabled(&self) -> bool {
        false
    }

    fn record(&self, _event: AuditEvent<'_>) -> AuditAdmission {
        AuditAdmission::immediate()
    }
}
