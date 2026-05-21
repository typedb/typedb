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
        Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use resource::constants::diagnostics::UNKNOWN_STR;
use serde::{Deserialize, Serialize};
use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System};

use crate::{
    DatabaseHash, DatabaseHashOpt,
    reports::{
        ActionReport, ConnectionLoadReport, DataLoadReport, ErrorReport, LoadReport, OsReport, SchemaLoadReport,
        ServerPropertiesReport, ServerReport, ServerReportSensitivePart,
    },
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

use crate::reports::DatabaseReport;

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
    // Reused sysinfo handles. Behaviour-preserving: each scrape still refreshes,
    // but we no longer allocate a fresh System / Disks on every /diagnostics request.
    // Phase 3 will replace the on-scrape refresh path with a background sampler
    // that writes cached AtomicU64 values, after which to_full_state_report will
    // read the atomics directly without taking the Mutex.
    system: Mutex<System>,
    disks: Mutex<Disks>,
}

impl ServerMetrics {
    pub(crate) fn new(version: String, data_directory: PathBuf) -> ServerMetrics {
        let os_name = System::name().unwrap_or(UNKNOWN_STR.to_string());
        let os_arch = System::cpu_arch();
        let os_version = System::os_version().unwrap_or(UNKNOWN_STR.to_string());
        let system = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
        let disks = Disks::new_with_refreshed_list();
        Self {
            start_instant: Instant::now(),
            os_name,
            os_arch,
            os_version,
            version,
            data_directory,
            system: Mutex::new(system),
            disks: Mutex::new(disks),
        }
    }

    pub fn data_directory(&self) -> &PathBuf {
        &self.data_directory
    }

    pub fn to_minimal_state_report(&self) -> ServerReport {
        ServerReport { version: self.version.clone(), sensitive_part: None }
    }

    pub fn to_full_state_report(&self) -> ServerReport {
        let memory_info = self.get_memory_info();
        let disk_info = self.get_disk_info();
        ServerReport {
            version: self.version.clone(),
            sensitive_part: Some(ServerReportSensitivePart {
                uptime_in_seconds: self.get_uptime_in_seconds(),
                os: OsReport {
                    name: self.os_name.clone(),
                    arch: self.os_arch.clone(),
                    version: self.os_version.clone(),
                },
                memory_used_in_bytes: memory_info.total - memory_info.available,
                memory_available_in_bytes: memory_info.available,
                disk_used_in_bytes: disk_info.total - disk_info.available,
                disk_available_in_bytes: disk_info.available,
            }),
        }
    }

    fn get_memory_info(&self) -> SizeInfo {
        let mut system = self.system.lock().expect("Expected sysinfo System lock acquisition");
        system.refresh_memory();
        // USER QUESTION: can we include RSS of the server process?
        SizeInfo { total: system.total_memory(), available: system.available_memory() }
    }

    fn get_disk_info(&self) -> SizeInfo {
        let mut disks = self.disks.lock().expect("Expected sysinfo Disks lock acquisition");
        // remove_not_listed_disks = true preserves the original Disks::new_with_refreshed_list()
        // semantics: each scrape sees the disks currently present, not stale entries.

        // USER QUESTION: can we record the disk data directory size somehow? Eg. to record databases bytes or something
        disks.refresh(true);
        let disk = match self.data_directory.canonicalize() {
            Ok(path) => disks.iter().find(|disk| path.starts_with(disk.mount_point())),
            Err(_) => None,
        };

        match disk {
            Some(disk) => SizeInfo { total: disk.total_space(), available: disk.available_space() },
            None => SizeInfo { total: 0u64, available: 0u64 },
        }
    }

    fn get_uptime_in_seconds(&self) -> i64 {
        self.start_instant.elapsed().as_secs() as i64
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatabaseMetrics {
    pub database_name: String,
    pub schema: SchemaLoadMetrics,
    pub data: DataLoadMetrics,
}

#[derive(Debug)]
pub(crate) struct LoadMetrics {
    schema: SchemaLoadMetrics,
    data: DataLoadMetrics,
    connection: ConnectionLoadMetrics,
    is_deleted: bool,
}

impl LoadMetrics {
    pub fn new() -> Self {
        Self {
            schema: SchemaLoadMetrics { type_count: 0 },
            data: DataLoadMetrics {
                entity_count: 0,
                relation_count: 0,
                attribute_count: 0,
                has_count: 0,
                role_count: 0,
                storage_in_bytes: 0,
                storage_key_count: 0,
            },
            connection: ConnectionLoadMetrics::new(),
            is_deleted: false,
        }
    }

    pub fn set_schema(&mut self, schema: SchemaLoadMetrics) {
        self.is_deleted = false;
        self.schema = schema;
    }

    pub fn set_data(&mut self, data: DataLoadMetrics) {
        self.is_deleted = false;
        self.data = data;
    }

    // USER COMMENT: ok, let's go back to the old established LoadKind apporach instead of TransactionType, and we can refine it later.
    //               It will make the diff smaller and easier to review!
    pub fn increment_connection_count(&self, client: ClientEndpoint, transaction_type: TransactionType) {
        self.connection.increment_count(client, transaction_type);
    }

    pub fn decrement_connection_count(&self, client: ClientEndpoint, transaction_type: TransactionType) {
        self.connection.decrement_count(client, transaction_type);
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

    pub fn to_peak_report(&self, database_hash: &DatabaseHash) -> Option<LoadReport> {
        if !self.is_deleted || !self.connection.is_empty() {
            let mut report = LoadReport::new(*database_hash);
            report.connection = Some(self.connection.to_peak_report());
            report.schema = Some(self.schema.to_state_report());
            report.data = Some(self.data.to_state_report());
            Some(report)
        } else {
            None
        }
    }

    pub fn to_state_report(&self, database_hash: &DatabaseHash) -> Option<LoadReport> {
        if !self.is_deleted {
            let mut report = LoadReport::new(*database_hash);
            report.schema = Some(self.schema.to_state_report());
            report.data = Some(self.data.to_state_report());
            Some(report)
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SchemaLoadMetrics {
    pub type_count: u64,
}

impl SchemaLoadMetrics {
    pub fn to_state_report(&self) -> SchemaLoadReport {
        SchemaLoadReport { type_count: self.type_count }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
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
    counts: HashMap<ClientEndpoint, HashMap<TransactionType, AtomicU64>>,
    peak_counts: HashMap<ClientEndpoint, HashMap<TransactionType, AtomicU64>>,
    backup_peak_counts: HashMap<ClientEndpoint, HashMap<TransactionType, AtomicU64>>,
}

impl ConnectionLoadMetrics {
    pub fn new() -> Self {
        Self {
            counts: client_endpoints_map!(TransactionType::all_empty_counts_map()),
            peak_counts: client_endpoints_map!(TransactionType::all_empty_counts_map()),
            backup_peak_counts: client_endpoints_map!(TransactionType::all_empty_counts_map()),
        }
    }

    pub fn increment_count(&self, client: ClientEndpoint, transaction_type: TransactionType) {
        let old_count = self.get_count(&client, &transaction_type).fetch_add(1, Ordering::Relaxed);
        self.update_peak_counts(client, transaction_type, old_count + 1);
    }

    pub fn decrement_count(&self, client: ClientEndpoint, transaction_type: TransactionType) {
        let old_count = self.get_count(&client, &transaction_type).fetch_sub(1, Ordering::Relaxed);
        assert_ne!(old_count, 0, "Attempted to decrement a zero count");
    }

    pub fn update_peak_counts(&self, client: ClientEndpoint, transaction_type: TransactionType, count: u64) {
        let peak_entry = self.get_peak_count(&client, &transaction_type);
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

    fn get_count(&self, client: &ClientEndpoint, transaction_type: &TransactionType) -> &AtomicU64 {
        self.counts
            .get(client)
            .expect("Expected client {client}")
            .get(transaction_type)
            .expect("Load keys should be preinserted")
    }

    fn get_peak_count(&self, client: &ClientEndpoint, transaction_type: &TransactionType) -> &AtomicU64 {
        self.peak_counts
            .get(client)
            .expect("Expected client {client}")
            .get(transaction_type)
            .expect("Load peak keys should be preinserted")
    }

    fn get_backup_peak_counts(&self, client: &ClientEndpoint, transaction_type: &TransactionType) -> &AtomicU64 {
        self.backup_peak_counts
            .get(client)
            .expect("Expected client {client}")
            .get(transaction_type)
            .expect("Load peak keys should be preinserted")
    }
}

#[derive(Debug)]
pub(crate) struct ActionMetrics {
    actions: HashMap<ActionKind, ActionInfo>,
    actions_snapshot: HashMap<ActionKind, ActionInfo>,
    actions_snapshot_backup: HashMap<ActionKind, ActionInfo>, // in case if reporting fails
}

impl ActionMetrics {
    pub fn new() -> Self {
        Self {
            actions: ActionKind::all_empty_counts_map(),
            actions_snapshot: ActionKind::all_empty_counts_map(),
            actions_snapshot_backup: ActionKind::all_empty_counts_map(),
        }
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

    pub fn to_diff_reports(&self, database_hash: DatabaseHashOpt) -> Vec<ActionReport> {
        let mut actions = vec![];
        for kind in self.actions.keys() {
            let successful = self.get_successful_delta(kind);
            let failed = self.get_failed_delta(kind);
            if successful == 0 && failed == 0 {
                continue;
            }
            actions.push(ActionReport {
                database: database_hash.map(|hash| DatabaseReport(hash)),
                kind: *kind,
                successful,
                failed,
            });
        }
        actions
    }

    pub fn to_state_reports(&self, database_hash: &DatabaseHashOpt) -> Vec<ActionReport> {
        let mut actions = vec![];
        for kind in self.actions.keys() {
            let successful = self.get_successful(kind) as i64;
            let failed = self.get_failed(kind) as i64;
            if successful == 0 && failed == 0 {
                continue;
            }
            actions.push(ActionReport { database: database_hash.map(DatabaseReport), kind: *kind, successful, failed });
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

#[derive(Debug)]
pub(crate) struct ErrorMetrics {
    errors: RwLock<HashMap<String, ErrorInfo>>,
    errors_snapshot: RwLock<HashMap<String, ErrorInfo>>,
    errors_snapshot_backup: RwLock<HashMap<String, ErrorInfo>>,
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self {
            errors: RwLock::new(HashMap::new()),
            errors_snapshot: RwLock::new(HashMap::new()),
            errors_snapshot_backup: RwLock::new(HashMap::new()),
        }
    }

    pub fn submit(&self, error_code: String) {
        self.get_errors_mut().entry(error_code).or_insert(ErrorInfo::new()).submit();
    }

    fn get_count_delta(&self, error_code: &str) -> i64 {
        get_delta(
            self.get_errors().get(error_code).unwrap_or(&ErrorInfo::default()).count,
            self.get_errors_snapshot().get(error_code).unwrap_or(&ErrorInfo::default()).count,
        )
    }

    pub fn take_snapshot(&mut self) {
        let errors = self.get_errors();
        let mut snapshot = self.get_errors_snapshot_mut();
        let mut backup = self.get_errors_snapshot_backup_mut();

        *backup = snapshot.clone();
        for (code, info) in errors.iter() {
            snapshot.insert(code.clone(), info.clone());
        }
    }

    pub fn restore_snapshot(&self) {
        let backup = self.get_errors_snapshot_backup().clone();
        let mut snapshot = self.get_errors_snapshot_mut();
        *snapshot = backup;
    }

    pub fn to_diff_reports(&self, database_hash: DatabaseHashOpt) -> Vec<ErrorReport> {
        let mut errors = vec![];
        for code in self.get_errors().keys() {
            let count = self.get_count_delta(code);
            if count == 0 {
                continue;
            }
            errors.push(ErrorReport {
                database: database_hash.map(|hash| DatabaseReport(hash)),
                code: code.clone(),
                count,
            });
        }
        errors
    }

    pub fn to_state_reports(&self, database_hash: &DatabaseHashOpt) -> Vec<ErrorReport> {
        let mut errors = vec![];
        for (code, info) in self.get_errors().iter() {
            assert_ne!(info.count, 0, "Error count cannot be 0");
            errors.push(ErrorReport {
                database: database_hash.map(|hash| DatabaseReport(hash)),
                code: code.clone(),
                count: info.count as i64,
            });
        }
        errors
    }

    pub fn get_errors(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors.read().expect("Expected error metrics read lock acquisition")
    }

    pub fn get_errors_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors.write().expect("Expected error metrics write lock acquisition")
    }

    pub fn get_errors_snapshot(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot.read().expect("Expected error metrics snapshot read lock acquisition")
    }

    pub fn get_errors_snapshot_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot.write().expect("Expected error metrics snapshot write lock acquisition")
    }

    pub fn get_errors_snapshot_backup(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot_backup.read().expect("Expected error metrics snapshot backup read lock acquisition")
    }

    pub fn get_errors_snapshot_backup_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot_backup.write().expect("Expected error metrics snapshot backup write lock acquisition")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    count: u64,
}

impl ErrorInfo {
    pub const fn new() -> Self {
        Self::default()
    }

    pub const fn default() -> Self {
        Self { count: 0 }
    }

    pub fn submit(&mut self) {
        self.count += 1;
    }
}

/// Identifies the kind of a transaction across the diagnostics crate. Mirrors
/// the server crate's own `TransactionType` (server/transaction.rs) — kept as a
/// parallel definition here because the dependency direction is server →
/// diagnostics, not the other way round, and we don't want diagnostics to
/// depend on server.
///
/// Variant names match the server enum (Read/Write/Schema). The wire-format
/// strings used by Posthog (`to_posthog_name`) and the historical Display
/// representation are unchanged — only the enum identifier changed from
/// `TransactionType` to `TransactionType` for clarity and naming alignment.
#[derive(Serialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Schema,
    Read,
    Write,
    // ATTENTION: When adding new variants, update all_empty_counts_map()!
}

impl TransactionType {
    fn all_empty_counts_map() -> HashMap<TransactionType, AtomicU64> {
        HashMap::from([
            (TransactionType::Schema, AtomicU64::new(0)),
            (TransactionType::Write, AtomicU64::new(0)),
            (TransactionType::Read, AtomicU64::new(0)),
        ])
    }

    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            TransactionType::Schema => "schema_transactions_peak_count",
            TransactionType::Read => "read_transactions_peak_count",
            TransactionType::Write => "write_transactions_peak_count",
        }
    }
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Schema => write!(f, "schemaTransactionPeakCount"),
            TransactionType::Read => write!(f, "readTransactionPeakCount"),
            TransactionType::Write => write!(f, "writeTransactionPeakCount"),
        }
    }
}

/// Identifies the kind of a query for the query-duration histogram. Mirrors
/// the server crate's `QueryType` (server/service/mod.rs). Used as the label
/// dimension on `typedb_query_duration_seconds`.
#[derive(Serialize, Debug, Hash, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
    Read,
    Write,
    Schema,
}

impl QueryType {
    /// Lowercase string for the Prometheus `kind` label. Matches the variant set
    /// `TransactionType` uses, keeping dashboards able to join the two.
    pub fn as_label(&self) -> &'static str {
        match self {
            QueryType::Read => "read",
            QueryType::Write => "write",
            QueryType::Schema => "schema",
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

fn get_delta(lhs: u64, rhs: u64) -> i64 {
    if lhs > rhs { (lhs - rhs) as i64 } else { -((rhs - lhs) as i64) }
}

struct SizeInfo {
    pub total: u64,
    pub available: u64,
}

// ============================================================================
// Histogram primitive
// ============================================================================

/// Default bucket boundaries for all duration histograms, in nanoseconds.
/// Covers six orders of magnitude (10µs … 10s) with one bucket per decade; broad
/// enough to catch sub-100µs point queries and 10s+ pathological tails, narrow
/// enough to keep per-histogram cardinality at 9 series (8 buckets + +Inf).
/// `+Inf` is implicit — observations above the last bound land in an overflow
/// bucket emitted as `le="+Inf"` at exposition time.
pub const DEFAULT_DURATION_BUCKETS_NANOS: &[u64] = &[
    10_000,         //  10 µs
    100_000,        // 100 µs
    1_000_000,      //   1 ms
    10_000_000,     //  10 ms
    100_000_000,    // 100 ms
    1_000_000_000,  //   1 s
    10_000_000_000, //  10 s
];

// USER COMMENT: let's add one more bucket for 10-100s, then 100s+

/// Bucket boundaries for the queries-per-transaction histogram. Counts, not
/// durations — same storage type, different scale. Mixes powers of ten with
/// half-decade points to catch both healthy short transactions and the
/// accidental-n+1 patterns the metric is meant to detect.
pub const DEFAULT_QUERIES_PER_TRANSACTION_BUCKETS: &[u64] = &[1, 2, 5, 10, 25, 100, 500, 1000];

// USER COMMENT: for queries use buckets, 1, 5, 10, 25, 100, 1000, 10000

/// Whether a histogram's stored u64 values represent nanoseconds (to be emitted
/// as seconds in Prometheus output) or raw counts (emitted as-is).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum HistogramUnit {
    Nanoseconds,
    Count,
}

/// Lock-free histogram with fixed bucket boundaries declared at construction.
/// `observe()` is the hot path: one atomic fetch_add on the chosen bucket (or
/// the overflow slot) and one on `sum`. The total observation count is derived
/// at snapshot time as the sum of all buckets + overflow — avoids a separate
/// `count` atomic that could disagree with the bucket sum under concurrent
/// observe (Relaxed loads are not synchronised across atomics). Bucket lookup
/// is a linear scan — fine for the 7-bound default; with more bounds a binary
/// search would be worth it.
///
/// Storage uses `u64` throughout. Durations are stored as nanoseconds (~584
/// years of headroom in `sum`); counts are stored as themselves. The `unit`
/// field tells the exposition writer how to render the bounds and the `_sum`
/// line: `Nanoseconds` divides by 1e9 to emit seconds (Prometheus convention
/// is seconds for the `_seconds` suffix), `Count` emits as-is.
#[derive(Debug)]
pub struct HistogramMetrics {
    bucket_bounds: Vec<u64>,
    bucket_counts: Vec<AtomicU64>,
    overflow_count: AtomicU64,
    sum: AtomicU64,
    unit: HistogramUnit,
}

impl HistogramMetrics {
    /// Construct with explicit bucket boundaries (in the unit's native u64).
    /// Boundaries are inclusive upper bounds; an observation of exactly `bounds[i]`
    /// counts in bucket i. `+Inf` is implicit — anything above the last bound
    /// counts in the overflow bucket.
    pub fn new(bucket_bounds: Vec<u64>, unit: HistogramUnit) -> Self {
        debug_assert!(
            bucket_bounds.windows(2).all(|w| w[0] < w[1]),
            "histogram bucket bounds must be strictly ascending"
        );
        let bucket_counts = (0..bucket_bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bucket_bounds,
            bucket_counts,
            overflow_count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            unit,
        }
    }

    /// Constructor for duration histograms with the standard bucket set.
    pub fn new_duration() -> Self {
        Self::new(DEFAULT_DURATION_BUCKETS_NANOS.to_vec(), HistogramUnit::Nanoseconds)
    }

    /// Constructor for the queries-per-transaction histogram (count-shaped).
    pub fn new_queries_per_transaction() -> Self {
        Self::new(DEFAULT_QUERIES_PER_TRANSACTION_BUCKETS.to_vec(), HistogramUnit::Count)
    }

    /// Observe a duration. Saturates a Duration exceeding `u64::MAX` ns
    /// (~584 years) into the overflow bucket — not a realistic timing.
    pub fn observe_duration(&self, d: std::time::Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        debug_assert!(self.unit == HistogramUnit::Nanoseconds);
        self.observe_raw(nanos);
    }

    /// Observe a raw count.
    pub fn observe_count(&self, n: u64) {
        debug_assert!(self.unit == HistogramUnit::Count);
        self.observe_raw(n);
    }

    fn observe_raw(&self, value: u64) {
        // Linear scan: the standard bucket sets are 7-9 bounds. If a future bucket
        // set grows beyond ~32 bounds, switch to partition_point binary search.
        let bucket = self.bucket_bounds.iter().position(|&b| value <= b);
        match bucket {
            Some(i) => self.bucket_counts[i].fetch_add(1, Ordering::Relaxed),
            None => self.overflow_count.fetch_add(1, Ordering::Relaxed),
        };
        // `sum` can wrap at u64::MAX (~584 years of accumulated ns). Realistic
        // soak windows are weeks, so no protection beyond the wrap is necessary.
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    pub fn unit(&self) -> HistogramUnit {
        self.unit
    }

    /// Snapshot for exposition. Produces *cumulative* bucket counts in the order
    /// the Prometheus text format expects, plus the total count and sum in raw
    /// (native-unit) form. The writer converts ns→seconds at emission time when
    /// `unit == Nanoseconds`.
    ///
    /// `count` is derived from the prefix-sum of buckets + overflow, so it
    /// equals `_bucket{le="+Inf"}` by construction. This avoids the bucket-vs-
    /// separate-counter skew that a separately-incremented `count` atomic
    /// would have under concurrent observe (Relaxed ordering doesn't
    /// synchronise across atomics).
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut cumulative_counts = Vec::with_capacity(self.bucket_bounds.len());
        let mut running = 0u64;
        for c in &self.bucket_counts {
            running += c.load(Ordering::Relaxed);
            cumulative_counts.push(running);
        }
        running += self.overflow_count.load(Ordering::Relaxed);
        // `running` is now the total observation count = +Inf bucket = `_count`.
        let plus_inf_count = running;

        // USER QUESTION: why do we have two fields that contain plus_inf_count
        HistogramSnapshot {
            bucket_bounds: self.bucket_bounds.clone(),
            cumulative_counts,
            plus_inf_count,
            count: plus_inf_count,
            sum: self.sum.load(Ordering::Relaxed),
            unit: self.unit,
        }
    }
}

/// Plain-data snapshot of a `HistogramMetrics` at one moment, used by the
/// JSON/Prometheus exposition. Cumulative counts; the +Inf bucket is separate.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub bucket_bounds: Vec<u64>,
    pub cumulative_counts: Vec<u64>,
    pub plus_inf_count: u64,
    pub count: u64,
    pub sum: u64,
    pub unit: HistogramUnit,
}

// ============================================================================
// Per-database histogram container + transaction lifecycle counters
// ============================================================================

/// Outcomes tracked by `TransactionLifecycleCounters`. Distinct from
/// `ActionKind::Transaction{Commit,Rollback}` which count RPC outcomes — these
/// count transaction *lifecycle* events. A transaction force-aborted on
/// timeout, for example, may never produce a TransactionCommit RPC failure,
/// but should still tick `aborted` here.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransactionOutcome {
    Started,
    Committed,
    RolledBack,
    Aborted, // USER COMMENT: 'closed' not 'aborted'
}

/// Per-TransactionType atomic counters for each lifecycle outcome. Same pattern
/// as `ConnectionLoadMetrics`: variants pre-inserted, observe() is lock-free.
#[derive(Debug)]
pub(crate) struct TransactionLifecycleCounters {
    started: HashMap<TransactionType, AtomicU64>,
    committed: HashMap<TransactionType, AtomicU64>,
    rolled_back: HashMap<TransactionType, AtomicU64>,
    aborted: HashMap<TransactionType, AtomicU64>,
}

impl TransactionLifecycleCounters {
    pub fn new() -> Self {
        fn zeros() -> HashMap<TransactionType, AtomicU64> {
            [TransactionType::Read, TransactionType::Write, TransactionType::Schema]
                .into_iter()
                .map(|tt| (tt, AtomicU64::new(0)))
                .collect()
        }
        Self { started: zeros(), committed: zeros(), rolled_back: zeros(), aborted: zeros() }
    }

    pub fn record(&self, kind: TransactionType, outcome: TransactionOutcome) {
        let table = match outcome {
            TransactionOutcome::Started => &self.started,
            TransactionOutcome::Committed => &self.committed,
            TransactionOutcome::RolledBack => &self.rolled_back,
            TransactionOutcome::Aborted => &self.aborted,
        };
        table.get(&kind).expect("All TransactionType variants pre-inserted").fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TransactionLifecycleSnapshot {
        let load = |t: &HashMap<TransactionType, AtomicU64>, k: TransactionType| t.get(&k).unwrap().load(Ordering::Relaxed);
        // Emit Read/Write/Schema in fixed order so exposition is deterministic.
        let kinds = [TransactionType::Read, TransactionType::Write, TransactionType::Schema];
        TransactionLifecycleSnapshot {
            started: kinds.into_iter().map(|k| (k, load(&self.started, k))).collect(),
            committed: kinds.into_iter().map(|k| (k, load(&self.committed, k))).collect(),
            rolled_back: kinds.into_iter().map(|k| (k, load(&self.rolled_back, k))).collect(),
            aborted: kinds.into_iter().map(|k| (k, load(&self.aborted, k))).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionLifecycleSnapshot {
    pub started: Vec<(TransactionType, u64)>,
    pub committed: Vec<(TransactionType, u64)>,
    pub rolled_back: Vec<(TransactionType, u64)>,
    pub aborted: Vec<(TransactionType, u64)>,
}

/// All Phase 2 per-database monitoring state: latency histograms, queries-per-
/// transaction, and the transaction-lifecycle counters. Held in one struct so
/// the per-DB map (`Diagnostics.histogram_metrics`) doesn't multiply. One
/// instance per database hash, constructed eagerly so observe() is lock-free
/// once the outer map is unlocked. None of this goes to Posthog; see §1.3 of
/// the Phase 2 design.
#[derive(Debug)]
pub(crate) struct DatabaseHistograms {
    query_duration: HashMap<QueryType, HistogramMetrics>,
    transaction_duration: HashMap<TransactionType, HistogramMetrics>,
    queries_per_transaction: HistogramMetrics,
    transaction_lifecycle: TransactionLifecycleCounters,
}

impl DatabaseHistograms {
    pub fn new() -> Self {
        let query_duration = [QueryType::Read, QueryType::Write, QueryType::Schema]
            .into_iter()
            .map(|qt| (qt, HistogramMetrics::new_duration()))
            .collect();
        let transaction_duration = [TransactionType::Read, TransactionType::Write, TransactionType::Schema]
            .into_iter()
            .map(|tt| (tt, HistogramMetrics::new_duration()))
            .collect();
        Self {
            query_duration,
            transaction_duration,
            queries_per_transaction: HistogramMetrics::new_queries_per_transaction(),
            transaction_lifecycle: TransactionLifecycleCounters::new(),
        }
    }

    pub fn observe_query_duration(&self, kind: QueryType, d: std::time::Duration) {
        self.query_duration.get(&kind).expect("All QueryType variants pre-inserted").observe_duration(d);
    }

    pub fn observe_transaction_duration(&self, kind: TransactionType, d: std::time::Duration) {
        self.transaction_duration.get(&kind).expect("All TransactionType variants pre-inserted").observe_duration(d);
    }

    pub fn observe_queries_per_transaction(&self, n: u64) {
        self.queries_per_transaction.observe_count(n);
    }

    pub fn record_transaction_outcome(&self, kind: TransactionType, outcome: TransactionOutcome) {
        self.transaction_lifecycle.record(kind, outcome);
    }

    /// Snapshot all histograms; returns paired (kind, snapshot) lists for the
    /// per-kind histograms and a single snapshot for queries-per-transaction.
    /// Exposition iterates the result; no locks are taken here beyond the brief
    /// per-bucket atomic loads inside `HistogramMetrics::snapshot`.
    pub fn snapshot(&self) -> DatabaseHistogramsSnapshot {
        let mut query_duration: Vec<_> =
            self.query_duration.iter().map(|(k, h)| (*k, h.snapshot())).collect();
        // Stable order so dashboards don't churn with HashMap iteration order.
        query_duration.sort_by_key(|(k, _)| match k {
            QueryType::Read => 0,
            QueryType::Write => 1,
            QueryType::Schema => 2,
        });
        let mut transaction_duration: Vec<_> =
            self.transaction_duration.iter().map(|(k, h)| (*k, h.snapshot())).collect();
        transaction_duration.sort_by_key(|(k, _)| match k {
            TransactionType::Read => 0,
            TransactionType::Write => 1,
            TransactionType::Schema => 2,
        });
        DatabaseHistogramsSnapshot {
            query_duration,
            transaction_duration,
            queries_per_transaction: self.queries_per_transaction.snapshot(),
            transaction_lifecycle: self.transaction_lifecycle.snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseHistogramsSnapshot {
    pub query_duration: Vec<(QueryType, HistogramSnapshot)>,
    pub transaction_duration: Vec<(TransactionType, HistogramSnapshot)>,
    pub queries_per_transaction: HistogramSnapshot,
    pub transaction_lifecycle: TransactionLifecycleSnapshot,
}

// ============================================================================
// Histogram unit tests
// ============================================================================

#[cfg(test)]
mod histogram_tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::{DEFAULT_DURATION_BUCKETS_NANOS, HistogramMetrics, HistogramUnit};

    #[test]
    fn bucketing_picks_the_smallest_upper_bound_that_includes_the_observation() {
        let h = HistogramMetrics::new_duration();
        // 50 µs → second bucket (le=100µs).
        h.observe_duration(Duration::from_micros(50));
        // Exactly 100 µs → still the second bucket (bounds are inclusive upper).
        h.observe_duration(Duration::from_micros(100));
        // 5 ms → fourth bucket (le=10ms).
        h.observe_duration(Duration::from_millis(5));
        // 20 s → overflow (above the 10s top bound).
        h.observe_duration(Duration::from_secs(20));

        let snap = h.snapshot();
        // Cumulative: bucket 0 (le=10µs) = 0, bucket 1 (le=100µs) = 2, ..., bucket 3 (le=10ms) = 3.
        assert_eq!(snap.cumulative_counts[0], 0, "10µs bucket should be empty");
        assert_eq!(snap.cumulative_counts[1], 2, "100µs bucket sees 50µs + 100µs");
        assert_eq!(snap.cumulative_counts[2], 2, "1ms bucket unchanged");
        assert_eq!(snap.cumulative_counts[3], 3, "10ms bucket adds the 5ms observation");
        assert_eq!(snap.cumulative_counts.last().copied().unwrap(), 3, "10s bucket = pre-overflow total");
        assert_eq!(snap.plus_inf_count, 4, "+Inf includes the 20s overflow");
        assert_eq!(snap.count, 4);
    }

    #[test]
    fn sum_accumulates_in_native_units() {
        let h = HistogramMetrics::new_duration();
        h.observe_duration(Duration::from_micros(50));
        h.observe_duration(Duration::from_millis(5));
        let snap = h.snapshot();
        // 50 µs + 5 ms = 5_050_000 ns
        assert_eq!(snap.sum, 50_000 + 5_000_000);
        assert_eq!(snap.unit, HistogramUnit::Nanoseconds);
    }

    #[test]
    fn empty_histogram_snapshots_with_zeros_everywhere() {
        let h = HistogramMetrics::new_duration();
        let snap = h.snapshot();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.sum, 0);
        assert_eq!(snap.plus_inf_count, 0);
        assert!(snap.cumulative_counts.iter().all(|&c| c == 0));
        // Bucket bound set matches the default duration buckets.
        assert_eq!(snap.bucket_bounds.as_slice(), DEFAULT_DURATION_BUCKETS_NANOS);
    }

    #[test]
    fn count_histogram_observes_u64_directly() {
        let h = HistogramMetrics::new_queries_per_transaction();
        h.observe_count(1);
        h.observe_count(5);
        h.observe_count(2000); // above last bound (1000) → overflow
        let snap = h.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.sum, 1 + 5 + 2000);
        // Buckets are [1, 2, 5, 10, 25, 100, 500, 1000]. The observation of 1 falls in
        // bucket 0 (le=1); 5 falls in bucket 2 (le=5); 2000 overflows.
        assert_eq!(snap.cumulative_counts[0], 1);
        assert_eq!(snap.cumulative_counts[1], 1);
        assert_eq!(snap.cumulative_counts[2], 2);
        assert_eq!(snap.plus_inf_count, 3);
    }

    #[test]
    fn concurrent_observers_do_not_lose_observations() {
        // 8 threads × 1000 observations each = 8000 total. Each thread observes a
        // value that lands in a different bucket, so we can also confirm bucketing
        // under contention.
        let h = Arc::new(HistogramMetrics::new_duration());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let h = h.clone();
                thread::spawn(move || {
                    // Pick a value that maps deterministically to bucket `t % 7`.
                    let nanos = DEFAULT_DURATION_BUCKETS_NANOS[t % 7];
                    for _ in 0..1000 {
                        h.observe_duration(Duration::from_nanos(nanos));
                    }
                })
            })
            .collect();
        for j in threads {
            j.join().unwrap();
        }
        let snap = h.snapshot();
        assert_eq!(snap.count, 8000, "no observation lost under contention");
        // sum_nanos must equal the sum of (1000 × nanos_for_thread_t).
        let expected_sum: u64 =
            (0..8u64).map(|t| 1000u64 * DEFAULT_DURATION_BUCKETS_NANOS[(t as usize) % 7]).sum();
        assert_eq!(snap.sum, expected_sum);
    }
}
