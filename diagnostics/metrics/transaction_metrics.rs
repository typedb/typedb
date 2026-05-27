/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use tokio::time::Instant;

use crate::{
    diagnostics_manager::DiagnosticsManager,
    metrics::{ClientEndpoint, LoadKind, QueryType, TransactionOutcome},
};

#[derive(Debug)]
pub struct TransactionMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<str>,
    kind: LoadKind,
    client: ClientEndpoint,
    opened_at: Instant,
    query_count: u64,
    terminal_outcome: Option<TransactionOutcome>,
}

impl TransactionMetrics {
    pub fn new(
        diagnostics_manager: Arc<DiagnosticsManager>,
        database_name: Arc<str>,
        kind: LoadKind,
        client: ClientEndpoint,
    ) -> Self {
        diagnostics_manager.increment_load_count(client, &database_name, kind);
        diagnostics_manager.record_transaction_outcome(&database_name, kind, TransactionOutcome::Started);
        Self {
            diagnostics_manager,
            database_name,
            kind,
            client,
            opened_at: Instant::now(),
            query_count: 0,
            terminal_outcome: None,
        }
    }

    pub fn record_query(&mut self) {
        self.query_count += 1;
    }

    pub fn database_name(&self) -> Arc<str> {
        Arc::clone(&self.database_name)
    }

    pub fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    pub fn mark_committed(&mut self) {
        self.terminal_outcome = Some(TransactionOutcome::Committed);
    }

    pub fn record_rolled_back(&self) {
        self.diagnostics_manager.record_transaction_outcome(
            &self.database_name,
            self.kind,
            TransactionOutcome::RolledBack,
        );
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        let outcome = self.terminal_outcome.take().unwrap_or(TransactionOutcome::Closed);
        self.diagnostics_manager.record_transaction_outcome(&self.database_name, self.kind, outcome);
        self.diagnostics_manager.observe_transaction_duration(&self.database_name, self.kind, self.opened_at.elapsed());
        self.diagnostics_manager.observe_queries_per_transaction(&self.database_name, self.query_count);
        self.diagnostics_manager.decrement_load_count(self.client, &self.database_name, self.kind);
    }
}

#[derive(Debug)]
pub struct WriteQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<str>,
    started_at: Instant,
}

impl WriteQueryMetrics {
    pub fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<str>) -> Self {
        Self { diagnostics_manager, database_name, started_at: Instant::now() }
    }
}

impl Drop for WriteQueryMetrics {
    fn drop(&mut self) {
        self.diagnostics_manager.observe_query_duration(
            &self.database_name,
            QueryType::Write,
            self.started_at.elapsed(),
        );
    }
}

#[derive(Debug)]
pub struct ReadQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<str>,
    first_observation_at: Option<Instant>,
}

impl ReadQueryMetrics {
    pub fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<str>) -> Self {
        Self { diagnostics_manager, database_name, first_observation_at: Some(Instant::now()) }
    }

    pub fn observe_first_response(&mut self) {
        if let Some(start) = self.first_observation_at.take() {
            self.diagnostics_manager.observe_query_duration(&self.database_name, QueryType::Read, start.elapsed());
        }
    }
}

impl Drop for ReadQueryMetrics {
    fn drop(&mut self) {
        self.observe_first_response();
    }
}

#[derive(Debug)]
pub struct SchemaQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<str>,
    started_at: Instant,
}

impl SchemaQueryMetrics {
    pub fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<str>) -> Self {
        Self { diagnostics_manager, database_name, started_at: Instant::now() }
    }

    pub fn observe_finished(self) {
        self.diagnostics_manager.observe_query_duration(
            &self.database_name,
            QueryType::Schema,
            self.started_at.elapsed(),
        );
    }
}
