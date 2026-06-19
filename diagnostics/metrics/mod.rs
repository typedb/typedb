/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use concurrency::IntervalRunner;
use resource::constants::{diagnostics::UNKNOWN_STR, server::SYSTEM_METRICS_REFRESH_INTERVAL};
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::{
    DatabaseId,
    reports::{
        ActionReport, ConnectionLoadReport, DataLoadReport, LoadReport, OsReport, ProcessReport, SchemaLoadReport,
        ServerPropertiesReport, ServerReport, ServerReportSensitivePart,
    },
};

pub mod core_metrics;
pub mod error_metrics;
pub mod file_metrics;
pub mod histogram_metrics;
mod system_sampler;
pub mod transaction_metrics;
pub use core_metrics::CoreMetrics;
pub(crate) use error_metrics::ErrorMetrics;
pub use file_metrics::FsyncMetrics;
pub use histogram_metrics::{HistogramMetrics, HistogramSnapshot, HistogramUnit};
use system_sampler::SystemSampler;
pub(crate) use transaction_metrics::TransactionLifecycleCounters;
pub use transaction_metrics::{
    LoadKind, QueryType, ReadQueryMetrics, SchemaQueryMetrics, TransactionLifecycleSnapshot, TransactionMetrics,
    TransactionOutcome, WriteQueryMetrics,
};

#[derive(Serialize, Deserialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientEndpoint {
    Grpc,
    Http,
    // ATTENTION: When adding new ClientEndpoints, update ALL_CLIENT_ENDPOINTS!
}

pub(crate) const ALL_CLIENT_ENDPOINTS: [ClientEndpoint; 2] = [ClientEndpoint::Grpc, ClientEndpoint::Http];

impl fmt::Display for ClientEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientEndpoint::Grpc => write!(f, "grpc"),
            ClientEndpoint::Http => write!(f, "http"),
        }
    }
}

macro_rules! client_endpoints_map {
    ($value:expr) => {
        ALL_CLIENT_ENDPOINTS.iter().map(|client| (*client, $value)).collect::<HashMap<_, _>>()
    };
}
pub(crate) use client_endpoints_map;

#[derive(Debug)]
pub(crate) struct ServerProperties {
    deployment_id: String,
    server_id: String,
    distribution: String,
    is_reporting_enabled: bool,
}

impl ServerProperties {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        is_reporting_enabled: bool,
    ) -> ServerProperties {
        Self { deployment_id, server_id, distribution, is_reporting_enabled }
    }

    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    pub fn is_reporting_enabled(&self) -> bool {
        self.is_reporting_enabled
    }

    pub fn to_state_report(&self) -> ServerPropertiesReport {
        ServerPropertiesReport {
            deployment_id: self.deployment_id.clone(),
            server_id: self.server_id.clone(),
            distribution: self.distribution.clone(),
            enabled: self.is_reporting_enabled,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServerMetrics {
    start_instant: Instant,
    os_name: String,
    os_arch: String,
    os_version: String,
    version: String,
    data_directory: PathBuf,
    sampler: Arc<SystemSampler>,
    _sampler_refresh: IntervalRunner,
}

impl ServerMetrics {
    pub(crate) fn new(version: String, data_directory: PathBuf) -> ServerMetrics {
        let os_name = System::name().unwrap_or(UNKNOWN_STR.to_string());
        let os_arch = System::cpu_arch();
        let os_version = System::os_version().unwrap_or(UNKNOWN_STR.to_string());
        let sampler = Arc::new(SystemSampler::new(data_directory.clone()));
        let sampler_for_refresh = sampler.clone();
        let _sampler_refresh =
            IntervalRunner::new(move || sampler_for_refresh.refresh(), SYSTEM_METRICS_REFRESH_INTERVAL);
        Self {
            start_instant: Instant::now(),
            os_name,
            os_arch,
            os_version,
            version,
            data_directory,
            sampler,
            _sampler_refresh,
        }
    }

    pub fn data_directory(&self) -> &PathBuf {
        &self.data_directory
    }

    pub fn to_minimal_state_report(&self) -> ServerReport {
        ServerReport { version: self.version.clone(), sensitive_part: None }
    }

    pub fn to_full_state_report(&self) -> ServerReport {
        let total_memory = self.sampler.total_memory_bytes();
        let available_memory = self.sampler.available_memory_bytes();
        let disk_total = self.sampler.disk_total_bytes();
        let disk_available = self.sampler.disk_available_bytes();
        ServerReport {
            version: self.version.clone(),
            sensitive_part: Some(ServerReportSensitivePart {
                uptime_in_seconds: self.get_uptime_in_seconds(),
                os: OsReport {
                    name: self.os_name.clone(),
                    arch: self.os_arch.clone(),
                    version: self.os_version.clone(),
                },
                // Saturating subtraction: total and available are loaded from
                // separate atomics, and although they're written under one lock
                // they're read independently, so a reader can observe a torn pair
                // across refreshes. Defensive against `available > total`.
                memory_used_in_bytes: total_memory.saturating_sub(available_memory),
                memory_available_in_bytes: available_memory,
                disk_used_in_bytes: disk_total.saturating_sub(disk_available),
                disk_available_in_bytes: disk_available,
                process: ProcessReport {
                    cpu_seconds_total: self.sampler.process_cpu_seconds_total(),
                    resident_memory_bytes: self.sampler.process_resident_memory_bytes(),
                    virtual_memory_bytes: self.sampler.process_virtual_memory_bytes(),
                    start_time_unix_seconds: self.sampler.process_start_time_unix_seconds(),
                },
            }),
        }
    }

    fn get_uptime_in_seconds(&self) -> i64 {
        self.start_instant.elapsed().as_secs() as i64
    }
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct DatabaseMetricsSnapshot {
    pub schema: SchemaLoadMetrics,
    pub data: DataLoadMetrics,
}

#[derive(Debug)]
pub(crate) struct LoadMetrics {
    database_id: Arc<DatabaseId>,
    snapshot: DatabaseMetricsSnapshot,
    connection: ConnectionLoadMetrics,
    is_deleted: bool,
}

impl LoadMetrics {
    pub fn new(database_id: Arc<DatabaseId>) -> Self {
        Self {
            database_id,
            snapshot: DatabaseMetricsSnapshot::default(),
            connection: ConnectionLoadMetrics::new(),
            is_deleted: false,
        }
    }

    pub fn database_id(&self) -> &Arc<DatabaseId> {
        &self.database_id
    }

    pub fn set_snapshot(&mut self, snapshot: DatabaseMetricsSnapshot) {
        self.is_deleted = false;
        self.snapshot = snapshot;
    }

    pub fn increment_connection_count(&self, client: ClientEndpoint, load_kind: LoadKind) {
        self.connection.increment_count(client, load_kind);
    }

    pub fn decrement_connection_count(&self, client: ClientEndpoint, load_kind: LoadKind) {
        self.connection.decrement_count(client, load_kind);
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    pub fn take_snapshot(&self) {
        self.connection.take_snapshot()
    }

    pub fn restore_snapshot(&self) {
        self.connection.restore_snapshot()
    }

    pub fn to_peak_report(&self) -> Option<LoadReport> {
        if !self.is_deleted || !self.connection.is_empty() {
            let mut report = LoadReport::new(self.database_id.clone());
            report.connection = Some(self.connection.to_peak_report());
            report.schema = Some(self.snapshot.schema.to_state_report());
            report.data = Some(self.snapshot.data.to_state_report());
            Some(report)
        } else {
            None
        }
    }

    pub fn to_state_report(&self) -> Option<LoadReport> {
        if !self.is_deleted {
            let mut report = LoadReport::new(self.database_id.clone());
            report.schema = Some(self.snapshot.schema.to_state_report());
            report.data = Some(self.snapshot.data.to_state_report());
            report.connection = Some(self.connection.to_active_report());
            Some(report)
        } else {
            None
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SchemaLoadMetrics {
    pub type_count: u64,
}

impl SchemaLoadMetrics {
    pub fn to_state_report(&self) -> SchemaLoadReport {
        SchemaLoadReport { type_count: self.type_count }
    }
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct DataLoadMetrics {
    pub entity_count: u64,
    pub relation_count: u64,
    pub attribute_count: u64,
    pub has_count: u64,
    pub role_count: u64,
    pub storage_in_bytes: u64,
    pub storage_key_count: u64,
}

impl DataLoadMetrics {
    pub fn to_state_report(&self) -> DataLoadReport {
        DataLoadReport {
            entity_count: self.entity_count,
            relation_count: self.relation_count,
            attribute_count: self.attribute_count,
            has_count: self.has_count,
            role_count: self.role_count,
            storage_in_bytes: self.storage_in_bytes,
            storage_key_count: self.storage_key_count,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionLoadMetrics {
    counts: HashMap<ClientEndpoint, HashMap<LoadKind, AtomicU64>>,
    peak_counts: HashMap<ClientEndpoint, HashMap<LoadKind, AtomicU64>>,
    backup_peak_counts: HashMap<ClientEndpoint, HashMap<LoadKind, AtomicU64>>,
}

impl ConnectionLoadMetrics {
    pub fn new() -> Self {
        Self {
            counts: client_endpoints_map!(LoadKind::all_empty_counts_map()),
            peak_counts: client_endpoints_map!(LoadKind::all_empty_counts_map()),
            backup_peak_counts: client_endpoints_map!(LoadKind::all_empty_counts_map()),
        }
    }

    pub fn increment_count(&self, client: ClientEndpoint, load_kind: LoadKind) {
        let old_count = self.get_count(&client, &load_kind).fetch_add(1, Ordering::Relaxed);
        self.update_peak_counts(client, load_kind, old_count + 1);
    }

    pub fn decrement_count(&self, client: ClientEndpoint, load_kind: LoadKind) {
        let old_count = self.get_count(&client, &load_kind).fetch_sub(1, Ordering::Relaxed);
        assert_ne!(old_count, 0, "Attempted to decrement a zero count");
    }

    pub fn update_peak_counts(&self, client: ClientEndpoint, load_kind: LoadKind, count: u64) {
        let peak_entry = self.get_peak_count(&client, &load_kind);
        loop {
            let current_peak_count = peak_entry.load(Ordering::Relaxed);
            if current_peak_count >= count {
                break;
            }
            if peak_entry.compare_exchange(current_peak_count, count, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                break;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.peak_counts.iter().all(|(_, counts)| counts.iter().all(|(_, count)| count.load(Ordering::Relaxed) == 0))
    }

    pub fn take_snapshot(&self) {
        for (client, peak_counts) in &self.peak_counts {
            for (kind, peak_count) in peak_counts {
                let current_peak_count = peak_count.load(Ordering::Relaxed);
                self.get_backup_peak_counts(client, kind).store(current_peak_count, Ordering::Relaxed);
                let current_count = self.get_count(client, kind).load(Ordering::Relaxed);
                peak_count.store(current_count, Ordering::Relaxed);
            }
        }
    }

    pub fn restore_snapshot(&self) {
        for (client, peak_counts) in &self.peak_counts {
            for (kind, peak_count) in peak_counts {
                let backup_peak_count = self.get_backup_peak_counts(client, kind).load(Ordering::Relaxed);
                peak_count.store(backup_peak_count, Ordering::Relaxed);
            }
        }
    }

    pub fn to_peak_report(&self) -> ConnectionLoadReport {
        let mut peaks = ConnectionLoadReport::new();
        for (client, peak_counts) in &self.peak_counts {
            let mut client_peaks = HashMap::new();
            for (kind, peak_count) in peak_counts {
                client_peaks.insert(*kind, peak_count.load(Ordering::Relaxed));
            }
            peaks.insert(*client, client_peaks);
        }
        peaks
    }

    pub fn to_active_report(&self) -> ConnectionLoadReport {
        let mut active = ConnectionLoadReport::new();
        for (client, counts) in &self.counts {
            let mut client_counts = HashMap::new();
            for (kind, count) in counts {
                client_counts.insert(*kind, count.load(Ordering::Relaxed));
            }
            active.insert(*client, client_counts);
        }
        active
    }

    fn get_count(&self, client: &ClientEndpoint, load_kind: &LoadKind) -> &AtomicU64 {
        self.counts
            .get(client)
            .expect("Expected client {client}")
            .get(load_kind)
            .expect("Load keys should be preinserted")
    }

    fn get_peak_count(&self, client: &ClientEndpoint, load_kind: &LoadKind) -> &AtomicU64 {
        self.peak_counts
            .get(client)
            .expect("Expected client {client}")
            .get(load_kind)
            .expect("Load peak keys should be preinserted")
    }

    fn get_backup_peak_counts(&self, client: &ClientEndpoint, load_kind: &LoadKind) -> &AtomicU64 {
        self.backup_peak_counts
            .get(client)
            .expect("Expected client {client}")
            .get(load_kind)
            .expect("Load peak keys should be preinserted")
    }
}

#[derive(Debug)]
pub(crate) struct ActionMetrics {
    /// `None` denotes the server-level (no-database) metrics record.
    database_id: Option<Arc<DatabaseId>>,
    actions: HashMap<ActionKind, ActionInfo>,
    actions_snapshot: HashMap<ActionKind, ActionInfo>,
    actions_snapshot_backup: HashMap<ActionKind, ActionInfo>, // in case if reporting fails
}

impl ActionMetrics {
    pub fn new(database_id: Option<Arc<DatabaseId>>) -> Self {
        Self {
            database_id,
            actions: ActionKind::all_empty_counts_map(),
            actions_snapshot: ActionKind::all_empty_counts_map(),
            actions_snapshot_backup: ActionKind::all_empty_counts_map(),
        }
    }

    pub fn database_id(&self) -> Option<&Arc<DatabaseId>> {
        self.database_id.as_ref()
    }

    pub fn submit_success(&self, action_kind: ActionKind) {
        self.get_action(&action_kind).submit_success();
    }
    pub fn submit_fail(&self, action_kind: ActionKind) {
        self.get_action(&action_kind).submit_fail();
    }

    fn get_successful(&self, action_kind: &ActionKind) -> u64 {
        self.get_action(action_kind).successful.load(Ordering::Relaxed)
    }

    fn get_failed(&self, action_kind: &ActionKind) -> u64 {
        self.get_action(action_kind).failed.load(Ordering::Relaxed)
    }

    fn get_attempted(&self, action_kind: &ActionKind) -> u64 {
        self.get_action(action_kind).get_attempted()
    }

    fn get_successful_delta(&self, action_kind: &ActionKind) -> i64 {
        get_delta(
            self.get_successful(action_kind),
            self.get_action_snapshot(action_kind).successful.load(Ordering::Relaxed),
        )
    }

    fn get_failed_delta(&self, action_kind: &ActionKind) -> i64 {
        get_delta(self.get_failed(action_kind), self.get_action_snapshot(action_kind).failed.load(Ordering::Relaxed))
    }

    pub fn take_snapshot(&mut self) {
        let all_kinds: HashSet<ActionKind> = self.actions.keys().cloned().collect();
        for kind in all_kinds {
            let current_value = self.get_action(&kind).clone();
            {
                let current_snapshot = self.get_action_snapshot(&kind).clone();
                let backup = self.get_action_snapshot_backup_mut(&kind);
                *backup = current_snapshot;
            }
            let snapshot = self.get_action_snapshot_mut(&kind);
            *snapshot = current_value;
        }
    }

    pub fn restore_snapshot(&mut self) {
        let all_kinds: HashSet<ActionKind> = self.actions.keys().cloned().collect();
        for kind in all_kinds {
            let backup = self.get_action_snapshot_backup(&kind).clone();
            let snapshot = self.get_action_snapshot_mut(&kind);
            *snapshot = backup;
        }
    }

    pub fn to_diff_reports(&self) -> Vec<ActionReport> {
        let mut actions = vec![];
        for kind in self.actions.keys() {
            let successful = self.get_successful_delta(kind);
            let failed = self.get_failed_delta(kind);
            if successful == 0 && failed == 0 {
                continue;
            }
            actions.push(ActionReport { database: self.database_id.clone(), kind: *kind, successful, failed });
        }
        actions
    }

    pub fn to_state_reports(&self) -> Vec<ActionReport> {
        let mut actions = vec![];
        for kind in self.actions.keys() {
            let successful = self.get_successful(kind) as i64;
            let failed = self.get_failed(kind) as i64;
            if successful == 0 && failed == 0 {
                continue;
            }
            actions.push(ActionReport { database: self.database_id.clone(), kind: *kind, successful, failed });
        }
        actions
    }

    fn get_action(&self, action_kind: &ActionKind) -> &ActionInfo {
        self.actions.get(action_kind).expect("Action keys should be preinserted")
    }

    fn get_action_snapshot(&self, action_kind: &ActionKind) -> &ActionInfo {
        self.actions_snapshot.get(action_kind).expect("Action snapshot keys should be preinserted")
    }

    fn get_action_snapshot_mut(&mut self, action_kind: &ActionKind) -> &mut ActionInfo {
        self.actions_snapshot.get_mut(action_kind).expect("Action snapshot keys should be preinserted")
    }

    fn get_action_snapshot_backup(&self, action_kind: &ActionKind) -> &ActionInfo {
        self.actions_snapshot_backup.get(action_kind).expect("Action snapshot backup keys should be preinserted")
    }

    fn get_action_snapshot_backup_mut(&mut self, action_kind: &ActionKind) -> &mut ActionInfo {
        self.actions_snapshot_backup.get_mut(action_kind).expect("Action snapshot backup keys should be preinserted")
    }
}

#[derive(Debug)]
pub(crate) struct ActionInfo {
    successful: AtomicU64,
    failed: AtomicU64,
}

impl ActionInfo {
    pub const fn default() -> Self {
        Self { successful: AtomicU64::new(0), failed: AtomicU64::new(0) }
    }

    pub fn submit_success(&self) {
        self.successful.fetch_add(1, Ordering::Relaxed);
    }

    pub fn submit_fail(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_successful(&self) -> u64 {
        self.successful.load(Ordering::Relaxed)
    }

    pub fn get_failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn get_attempted(&self) -> u64 {
        self.get_successful() + self.get_failed()
    }
}

impl Clone for ActionInfo {
    fn clone(&self) -> Self {
        Self {
            successful: AtomicU64::from(self.successful.load(Ordering::Relaxed)),
            failed: AtomicU64::from(self.failed.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Serialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionKind {
    // ATTENTION: When adding new Kinds, update all_empty_counts_map()!
    ConnectionOpen,
    SignIn,
    ServersAll,
    ServersGet,
    ServerVersion,
    UsersContains,
    UsersCreate,
    UsersUpdate,
    UsersDelete,
    UsersAll,
    UsersGet,
    Authenticate,
    DatabasesContains,
    DatabasesCreate,
    DatabasesImport,
    DatabasesGet,
    DatabasesAll,
    DatabaseSchema,
    DatabaseTypeSchema,
    DatabaseExport,
    DatabaseDelete,
    TransactionOpen,
    TransactionClose,
    TransactionCommit,
    TransactionRollback,
    TransactionAnalyse,
    TransactionQuery,
    OneshotQuery,
}

impl ActionKind {
    fn all_empty_counts_map() -> HashMap<Self, ActionInfo> {
        HashMap::from([
            (Self::ConnectionOpen, ActionInfo::default()),
            (Self::SignIn, ActionInfo::default()),
            (Self::ServersGet, ActionInfo::default()),
            (Self::ServersAll, ActionInfo::default()),
            (Self::ServerVersion, ActionInfo::default()),
            (Self::UsersContains, ActionInfo::default()),
            (Self::UsersCreate, ActionInfo::default()),
            (Self::UsersUpdate, ActionInfo::default()),
            (Self::UsersDelete, ActionInfo::default()),
            (Self::UsersAll, ActionInfo::default()),
            (Self::UsersGet, ActionInfo::default()),
            (Self::Authenticate, ActionInfo::default()),
            (Self::DatabasesContains, ActionInfo::default()),
            (Self::DatabasesCreate, ActionInfo::default()),
            (Self::DatabasesImport, ActionInfo::default()),
            (Self::DatabasesGet, ActionInfo::default()),
            (Self::DatabasesAll, ActionInfo::default()),
            (Self::DatabaseSchema, ActionInfo::default()),
            (Self::DatabaseTypeSchema, ActionInfo::default()),
            (Self::DatabaseExport, ActionInfo::default()),
            (Self::DatabaseDelete, ActionInfo::default()),
            (Self::TransactionOpen, ActionInfo::default()),
            (Self::TransactionClose, ActionInfo::default()),
            (Self::TransactionCommit, ActionInfo::default()),
            (Self::TransactionRollback, ActionInfo::default()),
            (Self::TransactionQuery, ActionInfo::default()),
            (Self::TransactionAnalyse, ActionInfo::default()),
            (Self::OneshotQuery, ActionInfo::default()),
        ])
    }

    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            ActionKind::ConnectionOpen => "connection_opens",
            ActionKind::SignIn => "sign_ins",
            ActionKind::ServersAll => "server_alls",
            ActionKind::ServersGet => "server_gets",
            ActionKind::ServerVersion => "server_versions",
            ActionKind::UsersContains => "user_containses",
            ActionKind::UsersCreate => "user_creates",
            ActionKind::UsersUpdate => "user_updates",
            ActionKind::UsersDelete => "user_deletes",
            ActionKind::UsersAll => "user_alls",
            ActionKind::UsersGet => "user_gets",
            ActionKind::Authenticate => "authenticates",
            ActionKind::DatabasesContains => "database_containses",
            ActionKind::DatabasesCreate => "database_creates",
            ActionKind::DatabasesImport => "database_imports",
            ActionKind::DatabasesGet => "database_gets",
            ActionKind::DatabasesAll => "database_alls",
            ActionKind::DatabaseSchema => "database_schemas",
            ActionKind::DatabaseTypeSchema => "database_type_schemas",
            ActionKind::DatabaseExport => "database_exports",
            ActionKind::DatabaseDelete => "databases_deletes",
            ActionKind::TransactionOpen => "transaction_opens",
            ActionKind::TransactionClose => "transaction_closes",
            ActionKind::TransactionCommit => "transaction_commits",
            ActionKind::TransactionRollback => "transaction_rollbacks",
            ActionKind::TransactionQuery => "transaction_queries",
            ActionKind::TransactionAnalyse => "transaction_analyses",
            ActionKind::OneshotQuery => "oneshot_queries",
        }
    }

    pub fn is_query(&self) -> bool {
        match self {
            ActionKind::TransactionQuery | ActionKind::OneshotQuery => true,
            _ => false,
        }
    }
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActionKind::ConnectionOpen => write!(f, "CONNECTION_OPEN"),
            ActionKind::SignIn => write!(f, "SIGN_IN"),
            ActionKind::ServersAll => write!(f, "SERVERS_ALL"),
            ActionKind::ServersGet => write!(f, "SERVERS_GET"),
            ActionKind::ServerVersion => write!(f, "SERVER_VERSION"),
            ActionKind::UsersContains => write!(f, "USERS_CONTAINS"),
            ActionKind::UsersCreate => write!(f, "USERS_CREATE"),
            ActionKind::UsersUpdate => write!(f, "USERS_UPDATE"),
            ActionKind::UsersDelete => write!(f, "USERS_DELETE"),
            ActionKind::UsersAll => write!(f, "USERS_ALL"),
            ActionKind::UsersGet => write!(f, "USERS_GET"),
            ActionKind::Authenticate => write!(f, "AUTHENTICATE"), // Analogue of 2.x's USER_TOKEN
            ActionKind::DatabasesContains => write!(f, "DATABASES_CONTAINS"),
            ActionKind::DatabasesCreate => write!(f, "DATABASES_CREATE"),
            ActionKind::DatabasesImport => write!(f, "DATABASES_IMPORT"),
            ActionKind::DatabasesGet => write!(f, "DATABASES_GET"),
            ActionKind::DatabasesAll => write!(f, "DATABASES_ALL"),
            ActionKind::DatabaseSchema => write!(f, "DATABASES_SCHEMA"),
            ActionKind::DatabaseTypeSchema => write!(f, "DATABASES_TYPE_SCHEMA"),
            ActionKind::DatabaseExport => write!(f, "DATABASES_EXPORT"),
            ActionKind::DatabaseDelete => write!(f, "DATABASES_DELETE"),
            ActionKind::TransactionOpen => write!(f, "TRANSACTION_OPEN"),
            ActionKind::TransactionClose => write!(f, "TRANSACTION_CLOSE"),
            ActionKind::TransactionCommit => write!(f, "TRANSACTION_COMMIT"),
            ActionKind::TransactionRollback => write!(f, "TRANSACTION_ROLLBACK"),
            ActionKind::TransactionQuery => write!(f, "TRANSACTION_QUERY"),
            ActionKind::TransactionAnalyse => write!(f, "TRANSACTION_ANALYSE"),
            ActionKind::OneshotQuery => write!(f, "ONESHOT_QUERY"),
        }
    }
}

pub(crate) fn get_delta(lhs: u64, rhs: u64) -> i64 {
    if lhs > rhs { (lhs - rhs) as i64 } else { -((rhs - lhs) as i64) }
}

// ============================================================================
// Histogram primitive bucket constants
// ============================================================================

/// Default bucket boundaries for all duration histograms, in nanoseconds.
/// One bucket per decade from 10µs to 100s; broad enough to catch sub-100µs
/// point queries and soak-pathology tails, narrow enough to keep per-histogram
/// cardinality at 10 series (9 buckets + +Inf). `+Inf` is implicit — observations
/// above the last bound land in an overflow bucket emitted as `le="+Inf"` at
/// exposition time.
pub const DEFAULT_DURATION_BUCKETS_NANOS: &[u64] = &[
    10_000,          //  10 µs
    100_000,         // 100 µs
    1_000_000,       //   1 ms
    10_000_000,      //  10 ms
    100_000_000,     // 100 ms
    1_000_000_000,   //   1 s
    10_000_000_000,  //  10 s
    100_000_000_000, // 100 s
];

/// Bucket boundaries for the queries-per-transaction histogram. Counts, not
/// durations — same storage type, different scale. Fewer mid-range buckets,
/// more headroom for accidental-n+1 outliers (top bound 10_000).
pub const DEFAULT_QUERIES_PER_TRANSACTION_BUCKETS: &[u64] = &[1, 5, 10, 25, 100, 1000, 10000];

// ============================================================================
// Per-database histogram container
// ============================================================================

#[derive(Debug)]
pub(crate) struct DatabaseHistograms {
    database_id: Arc<DatabaseId>,
    query_duration: HashMap<QueryType, HistogramMetrics>,
    transaction_duration: HashMap<LoadKind, HistogramMetrics>,
    queries_per_transaction: HistogramMetrics,
    transaction_lifecycle: TransactionLifecycleCounters,
    wal: FsyncMetrics,
}

impl DatabaseHistograms {
    pub fn new(database_id: Arc<DatabaseId>) -> Self {
        let query_duration = [QueryType::Read, QueryType::Write, QueryType::Schema]
            .into_iter()
            .map(|qt| (qt, HistogramMetrics::new_duration()))
            .collect();
        let transaction_duration =
            [LoadKind::ReadTransactions, LoadKind::WriteTransactions, LoadKind::SchemaTransactions]
                .into_iter()
                .map(|tt| (tt, HistogramMetrics::new_duration()))
                .collect();
        Self {
            database_id,
            query_duration,
            transaction_duration,
            queries_per_transaction: HistogramMetrics::new_queries_per_transaction(),
            transaction_lifecycle: TransactionLifecycleCounters::new(),
            wal: FsyncMetrics::new(),
        }
    }

    pub fn database_id(&self) -> &Arc<DatabaseId> {
        &self.database_id
    }

    pub fn observe_query_duration(&self, kind: QueryType, d: std::time::Duration) {
        self.query_duration.get(&kind).expect("All QueryType variants pre-inserted").observe_duration(d);
    }

    pub fn observe_transaction_duration(&self, kind: LoadKind, d: std::time::Duration) {
        self.transaction_duration.get(&kind).expect("All LoadKind variants pre-inserted").observe_duration(d);
    }

    pub fn observe_queries_per_transaction(&self, n: u64) {
        self.queries_per_transaction.observe_count(n);
    }

    pub fn record_transaction_outcome(&self, kind: LoadKind, outcome: TransactionOutcome) {
        self.transaction_lifecycle.record(kind, outcome);
    }

    pub fn wal_metrics(&self) -> FsyncMetrics {
        self.wal.clone()
    }

    pub fn snapshot(&self) -> DatabaseHistogramsSnapshot {
        let mut query_duration: Vec<_> = self.query_duration.iter().map(|(k, h)| (*k, h.snapshot())).collect();
        query_duration.sort_by_key(|(k, _)| match k {
            QueryType::Read => 0,
            QueryType::Write => 1,
            QueryType::Schema => 2,
        });
        let mut transaction_duration: Vec<_> =
            self.transaction_duration.iter().map(|(k, h)| (*k, h.snapshot())).collect();
        transaction_duration.sort_by_key(|(k, _)| match k {
            LoadKind::ReadTransactions => 0,
            LoadKind::WriteTransactions => 1,
            LoadKind::SchemaTransactions => 2,
        });
        DatabaseHistogramsSnapshot {
            query_duration,
            transaction_duration,
            queries_per_transaction: self.queries_per_transaction.snapshot(),
            transaction_lifecycle: self.transaction_lifecycle.snapshot(),
            wal_fsync_duration: self.wal.fsync_histogram_snapshot(),
            wal_bytes_written: self.wal.bytes_written(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseHistogramsSnapshot {
    pub query_duration: Vec<(QueryType, HistogramSnapshot)>,
    pub transaction_duration: Vec<(LoadKind, HistogramSnapshot)>,
    pub queries_per_transaction: HistogramSnapshot,
    pub transaction_lifecycle: TransactionLifecycleSnapshot,
    pub wal_fsync_duration: HistogramSnapshot,
    pub wal_bytes_written: u64,
}
