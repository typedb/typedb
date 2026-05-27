/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;
use tokio::time::Instant;

use crate::{diagnostics_manager::DiagnosticsManager, metrics::ClientEndpoint};

#[derive(Serialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub enum LoadKind {
    // Variant names are *Transactions for historical reasons; JSON exposition
    // strips the suffix so the wire-format `kind` field stays "read"/"write"/
    // "schema" — matching the Prometheus `kind` label and the server's own
    // TransactionType enum.
    #[serde(rename = "schema")]
    SchemaTransactions,
    #[serde(rename = "read")]
    ReadTransactions,
    #[serde(rename = "write")]
    WriteTransactions,
    // ATTENTION: When adding new variants, update all_empty_counts_map()!
}

impl LoadKind {
    pub(super) fn all_empty_counts_map() -> HashMap<LoadKind, AtomicU64> {
        HashMap::from([
            (LoadKind::SchemaTransactions, AtomicU64::new(0)),
            (LoadKind::WriteTransactions, AtomicU64::new(0)),
            (LoadKind::ReadTransactions, AtomicU64::new(0)),
        ])
    }

    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            LoadKind::SchemaTransactions => "schema_transactions_peak_count",
            LoadKind::ReadTransactions => "read_transactions_peak_count",
            LoadKind::WriteTransactions => "write_transactions_peak_count",
        }
    }
}

impl fmt::Display for LoadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadKind::SchemaTransactions => write!(f, "schemaTransactionPeakCount"),
            LoadKind::ReadTransactions => write!(f, "readTransactionPeakCount"),
            LoadKind::WriteTransactions => write!(f, "writeTransactionPeakCount"),
        }
    }
}

#[derive(Serialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
    Read,
    Write,
    Schema,
}

impl QueryType {
    /// Lowercase string for the Prometheus `kind` label. Matches the variant set
    /// `LoadKind` uses, keeping dashboards able to join the two.
    pub fn as_label(&self) -> &'static str {
        match self {
            QueryType::Read => "read",
            QueryType::Write => "write",
            QueryType::Schema => "schema",
        }
    }
}

/// Outcomes tracked by `TransactionLifecycleCounters`. Distinct from
/// `ActionKind::Transaction{Commit,Rollback}` which count RPC outcomes — these
/// count transaction *lifecycle* events. A transaction force-closed on
/// timeout, for example, may never produce a TransactionCommit RPC failure,
/// but should still tick `closed` here.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransactionOutcome {
    Started,
    Committed,
    RolledBack,
    Closed,
}

/// Per-LoadKind atomic counters for each lifecycle outcome. Same pattern
/// as `ConnectionLoadMetrics`: variants pre-inserted, observe() is lock-free.
#[derive(Debug)]
pub(crate) struct TransactionLifecycleCounters {
    started: HashMap<LoadKind, AtomicU64>,
    committed: HashMap<LoadKind, AtomicU64>,
    rolled_back: HashMap<LoadKind, AtomicU64>,
    closed: HashMap<LoadKind, AtomicU64>,
}

impl TransactionLifecycleCounters {
    pub fn new() -> Self {
        fn zeros() -> HashMap<LoadKind, AtomicU64> {
            [LoadKind::ReadTransactions, LoadKind::WriteTransactions, LoadKind::SchemaTransactions]
                .into_iter()
                .map(|tt| (tt, AtomicU64::new(0)))
                .collect()
        }
        Self { started: zeros(), committed: zeros(), rolled_back: zeros(), closed: zeros() }
    }

    pub fn record(&self, kind: LoadKind, outcome: TransactionOutcome) {
        let table = match outcome {
            TransactionOutcome::Started => &self.started,
            TransactionOutcome::Committed => &self.committed,
            TransactionOutcome::RolledBack => &self.rolled_back,
            TransactionOutcome::Closed => &self.closed,
        };
        table.get(&kind).expect("All LoadKind variants pre-inserted").fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TransactionLifecycleSnapshot {
        let load = |t: &HashMap<LoadKind, AtomicU64>, k: LoadKind| t.get(&k).unwrap().load(Ordering::Relaxed);
        // Emit Read/Write/Schema in fixed order so exposition is deterministic.
        let kinds = [LoadKind::ReadTransactions, LoadKind::WriteTransactions, LoadKind::SchemaTransactions];
        TransactionLifecycleSnapshot {
            started: kinds.into_iter().map(|k| (k, load(&self.started, k))).collect(),
            committed: kinds.into_iter().map(|k| (k, load(&self.committed, k))).collect(),
            rolled_back: kinds.into_iter().map(|k| (k, load(&self.rolled_back, k))).collect(),
            closed: kinds.into_iter().map(|k| (k, load(&self.closed, k))).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionLifecycleSnapshot {
    pub started: Vec<(LoadKind, u64)>,
    pub committed: Vec<(LoadKind, u64)>,
    pub rolled_back: Vec<(LoadKind, u64)>,
    pub closed: Vec<(LoadKind, u64)>,
}

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
