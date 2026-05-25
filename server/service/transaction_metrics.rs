/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use diagnostics::{
    diagnostics_manager::DiagnosticsManager,
    metrics::{ClientEndpoint, LoadKind, QueryType, TransactionOutcome},
};
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct TransactionMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: String,
    kind: LoadKind,
    client: ClientEndpoint,
    opened_at: Instant,
    query_count: u64,
    terminal_outcome: Option<TransactionOutcome>,
}

impl TransactionMetrics {
    pub(crate) fn new(
        diagnostics_manager: Arc<DiagnosticsManager>,
        database_name: String,
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

    pub(crate) fn record_query(&mut self) {
        self.query_count += 1;
    }

    pub(crate) fn database_name(&self) -> &str {
        &self.database_name
    }

    pub(crate) fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    pub(crate) fn mark_committed(&mut self) {
        self.terminal_outcome = Some(TransactionOutcome::Committed);
    }

    /// Non-terminal: a rolled-back transaction can be reused, so we fire the
    /// counter immediately rather than waiting for Drop. The terminal outcome
    /// (Committed or Closed) is still decided later.
    pub(crate) fn record_rolled_back(&self) {
        self.diagnostics_manager.record_transaction_outcome(
            &self.database_name,
            self.kind,
            TransactionOutcome::RolledBack,
        );
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        // Decrement the connection-active gauge first: the connection is closing
        // regardless of which terminal outcome we record next.
        self.diagnostics_manager.decrement_load_count(self.client, &self.database_name, self.kind);
        let outcome = self.terminal_outcome.take().unwrap_or(TransactionOutcome::Closed);
        self.diagnostics_manager.record_transaction_outcome(&self.database_name, self.kind, outcome);
        self.diagnostics_manager.observe_transaction_duration(
            &self.database_name,
            self.kind,
            self.opened_at.elapsed(),
        );
        self.diagnostics_manager.observe_queries_per_transaction(&self.database_name, self.query_count);
    }
}

#[derive(Debug)]
pub(crate) struct WriteQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: String,
    started_at: Instant,
}

impl WriteQueryMetrics {
    pub(crate) fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: String) -> Self {
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
