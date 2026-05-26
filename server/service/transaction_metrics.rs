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
struct LoadCountGuard {
    diagnostics_manager: Arc<DiagnosticsManager>,
    client: ClientEndpoint,
    database_name: Arc<String>,
    kind: LoadKind,
}

impl LoadCountGuard {
    fn new(
        diagnostics_manager: Arc<DiagnosticsManager>,
        client: ClientEndpoint,
        database_name: Arc<String>,
        kind: LoadKind,
    ) -> Self {
        diagnostics_manager.increment_load_count(client, database_name.as_str(), kind);
        Self { diagnostics_manager, client, database_name, kind }
    }
}

impl Drop for LoadCountGuard {
    fn drop(&mut self) {
        self.diagnostics_manager.decrement_load_count(self.client, self.database_name.as_str(), self.kind);
    }
}

#[derive(Debug)]
pub(crate) struct TransactionMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<String>,
    kind: LoadKind,
    opened_at: Instant,
    query_count: u64,
    terminal_outcome: Option<TransactionOutcome>,
    // Declared LAST so its Drop fires AFTER `TransactionMetrics::drop` runs.
    // (Rust drops fields in declaration order; the Drop impl body runs first.)
    _load_guard: LoadCountGuard,
}

impl TransactionMetrics {
    pub(crate) fn new(
        diagnostics_manager: Arc<DiagnosticsManager>,
        database_name: Arc<String>,
        kind: LoadKind,
        client: ClientEndpoint,
    ) -> Self {
        let _load_guard = LoadCountGuard::new(diagnostics_manager.clone(), client, database_name.clone(), kind);
        diagnostics_manager.record_transaction_outcome(database_name.as_str(), kind, TransactionOutcome::Started);
        Self {
            diagnostics_manager,
            database_name,
            kind,
            opened_at: Instant::now(),
            query_count: 0,
            terminal_outcome: None,
            _load_guard,
        }
    }

    pub(crate) fn record_query(&mut self) {
        self.query_count += 1;
    }

    pub(crate) fn database_name(&self) -> Arc<String> {
        Arc::clone(&self.database_name)
    }

    pub(crate) fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    pub(crate) fn mark_committed(&mut self) {
        self.terminal_outcome = Some(TransactionOutcome::Committed);
    }

    pub(crate) fn record_rolled_back(&self) {
        self.diagnostics_manager.record_transaction_outcome(
            self.database_name.as_str(),
            self.kind,
            TransactionOutcome::RolledBack,
        );
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        let outcome = self.terminal_outcome.take().unwrap_or(TransactionOutcome::Closed);
        self.diagnostics_manager.record_transaction_outcome(self.database_name.as_str(), self.kind, outcome);
        self.diagnostics_manager.observe_transaction_duration(
            self.database_name.as_str(),
            self.kind,
            self.opened_at.elapsed(),
        );
        self.diagnostics_manager.observe_queries_per_transaction(self.database_name.as_str(), self.query_count);
    }
}

#[derive(Debug)]
pub(crate) struct WriteQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<String>,
    started_at: Instant,
}

impl WriteQueryMetrics {
    pub(crate) fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<String>) -> Self {
        Self { diagnostics_manager, database_name, started_at: Instant::now() }
    }
}

impl Drop for WriteQueryMetrics {
    fn drop(&mut self) {
        self.diagnostics_manager.observe_query_duration(
            self.database_name.as_str(),
            QueryType::Write,
            self.started_at.elapsed(),
        );
    }
}

#[derive(Debug)]
pub(crate) struct ReadQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<String>,
    first_observation_at: Option<Instant>,
}

impl ReadQueryMetrics {
    pub(crate) fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<String>) -> Self {
        Self { diagnostics_manager, database_name, first_observation_at: Some(Instant::now()) }
    }

    pub(crate) fn observe_first_response(&mut self) {
        if let Some(start) = self.first_observation_at.take() {
            self.diagnostics_manager.observe_query_duration(
                self.database_name.as_str(),
                QueryType::Read,
                start.elapsed(),
            );
        }
    }
}

impl Drop for ReadQueryMetrics {
    fn drop(&mut self) {
        self.observe_first_response();
    }
}

#[derive(Debug)]
pub(crate) struct SchemaQueryMetrics {
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_name: Arc<String>,
    started_at: Instant,
}

impl SchemaQueryMetrics {
    pub(crate) fn new(diagnostics_manager: Arc<DiagnosticsManager>, database_name: Arc<String>) -> Self {
        Self { diagnostics_manager, database_name, started_at: Instant::now() }
    }

    pub(crate) fn observe_finished(self) {
        self.diagnostics_manager.observe_query_duration(
            self.database_name.as_str(),
            QueryType::Schema,
            self.started_at.elapsed(),
        );
    }
}
