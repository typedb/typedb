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

/// RAII pair for the connection-active gauge: the constructor increments,
/// Drop decrements. Owning this as a field on `TransactionMetrics` ensures
/// the gauge balances even if `TransactionMetrics::new` panics partway
/// through (e.g. on a poisoned lock during `record_transaction_outcome`):
/// the locally-bound guard unwinds and fires its Drop, balancing the
/// earlier increment.
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
        // Increment first via the guard. From here, any panic during this
        // constructor unwinds the local guard and balances the gauge.
        let _load_guard =
            LoadCountGuard::new(diagnostics_manager.clone(), client, database_name.clone(), kind);
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

    #[allow(dead_code)]
    pub(crate) fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    pub(crate) fn database_name_arc(&self) -> Arc<String> {
        Arc::clone(&self.database_name)
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
            self.database_name.as_str(),
            self.kind,
            TransactionOutcome::RolledBack,
        );
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        // The connection-active gauge is decremented by `_load_guard`'s Drop,
        // which fires AFTER this body returns. Record the terminal outcome
        // and final observations here.
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

    /// Fires typedb_query_duration_seconds{kind="read"} on the first call;
    /// subsequent calls are no-ops. Time-to-first-batch semantics: this is
    /// intended to be called at every read-response site except the
    /// init_ok_* headers (which are server-prepared markers, not first-batch
    /// content).
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

/// Single-shot schema-query timer: constructed at the start of the schema
/// query, consumed by `observe` at the single completion point. Consumes
/// `self` to enforce single-shot semantics — unlike `ReadQueryMetrics`,
/// which is called many times but only fires once.
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

    pub(crate) fn observe(self) {
        self.diagnostics_manager.observe_query_duration(
            self.database_name.as_str(),
            QueryType::Schema,
            self.started_at.elapsed(),
        );
    }
}
