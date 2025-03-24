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
        atomic::{AtomicU64, Ordering},
        RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    time::Instant,
};

use resource::constants::diagnostics::UNKNOWN_STR;
use serde::{Deserialize, Serialize};
use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System};

use crate::{
    reports::{
        ActionReport, ConnectionLoadReport, DataLoadReport, ErrorReport, LoadReport, OsReport, SchemaLoadReport,
        ServerPropertiesReport, ServerReport, ServerReportSensitivePart,
    },
    DatabaseHash, DatabaseHashOpt,
};

const MONITORING_VERSION: usize = 1;

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
}

impl ServerMetrics {
    pub(crate) fn new(version: String, data_directory: PathBuf) -> ServerMetrics {
        let os_name = System::name().unwrap_or(UNKNOWN_STR.to_string());
        let os_arch = System::cpu_arch();
        let os_version = System::os_version().unwrap_or(UNKNOWN_STR.to_string());
        Self { start_instant: Instant::now(), os_name, os_arch, os_version, version, data_directory }
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
        let system_info =
            System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
        SizeInfo { total: system_info.total_memory(), available: system_info.available_memory() }
    }

    fn get_disk_info(&self) -> SizeInfo {
        let disks = Disks::new_with_refreshed_list();
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
    pub is_primary_server: bool,
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

    pub fn to_peak_report(&self, database_hash: &DatabaseHash, is_owned: bool) -> Option<LoadReport> {
        if !self.is_deleted || !self.connection.is_empty() {
            let mut report = LoadReport::new(*database_hash);
            report.connection = Some(self.connection.to_peak_report());

            if is_owned {
                report.schema = Some(self.schema.to_state_report());
                report.data = Some(self.data.to_state_report());
            }

            Some(report)
        } else {
            None
        }
    }

    pub fn to_state_report(&self, database_hash: &DatabaseHash, is_owned: bool) -> Option<LoadReport> {
        if is_owned && !self.is_deleted {
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
        self.get_attempted(action_kind)
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
                let mut backup = self.get_action_snapshot_backup_mut(&kind);
                *backup = current_snapshot;
            }
            let mut snapshot = self.get_action_snapshot_mut(&kind);
            *snapshot = current_value;
        }
    }

    pub fn restore_snapshot(&mut self) {
        let all_kinds: HashSet<ActionKind> = self.actions.keys().cloned().collect();
        for kind in all_kinds {
            let backup = self.get_action_snapshot_backup(&kind).clone();
            let mut snapshot = self.get_action_snapshot_mut(&kind);
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
            actions.push(ActionReport {
                database: database_hash.map(|hash| DatabaseReport(hash)),
                kind: *kind,
                successful,
                failed,
            });
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

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActionKind::ConnectionOpen => write!(f, "CONNECTION_OPEN"),
            ActionKind::SignIn => write!(f, "SIGN_IN"),
            ActionKind::ServersAll => write!(f, "SERVERS_ALL"),
            ActionKind::UsersContains => write!(f, "USERS_CONTAINS"),
            ActionKind::UsersCreate => write!(f, "USERS_CREATE"),
            ActionKind::UsersUpdate => write!(f, "USERS_UPDATE"),
            ActionKind::UsersDelete => write!(f, "USERS_DELETE"),
            ActionKind::UsersAll => write!(f, "USERS_ALL"),
            ActionKind::UsersGet => write!(f, "USERS_GET"),
            ActionKind::Authenticate => write!(f, "AUTHENTICATE"), // Analogue of 2.x's USER_TOKEN
            ActionKind::DatabasesContains => write!(f, "DATABASES_CONTAINS"),
            ActionKind::DatabasesCreate => write!(f, "DATABASES_CREATE"),
            ActionKind::DatabasesGet => write!(f, "DATABASES_GET"),
            ActionKind::DatabasesAll => write!(f, "DATABASES_ALL"),
            ActionKind::DatabaseSchema => write!(f, "DATABASES_SCHEMA"),
            ActionKind::DatabaseTypeSchema => write!(f, "DATABASES_TYPE_SCHEMA"),
            ActionKind::DatabaseDelete => write!(f, "DATABASES_DELETE"),
            ActionKind::TransactionOpen => write!(f, "TRANSACTION_OPEN"),
            ActionKind::TransactionClose => write!(f, "TRANSACTION_CLOSE"),
            ActionKind::TransactionCommit => write!(f, "TRANSACTION_COMMIT"),
            ActionKind::TransactionRollback => write!(f, "TRANSACTION_ROLLBACK"),
            ActionKind::TransactionQuery => write!(f, "TRANSACTION_QUERY"),
            ActionKind::OneshotQuery => write!(f, "ONESHOT_QUERY"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActionInfo {
    successful: AtomicU64,
    failed: AtomicU64,
}

impl ActionInfo {
    pub const fn new() -> Self {
        Self::default()
    }

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

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub enum LoadKind {
    SchemaTransactions,
    ReadTransactions,
    WriteTransactions,
    // ATTENTION: When adding new Kinds, update all_empty_counts_map()!
}

impl LoadKind {
    fn all_empty_counts_map() -> HashMap<LoadKind, AtomicU64> {
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
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionKind {
    ConnectionOpen,
    SignIn,
    ServersAll,
    UsersContains,
    UsersCreate,
    UsersUpdate,
    UsersDelete,
    UsersAll,
    UsersGet,
    Authenticate,
    DatabasesContains,
    DatabasesCreate,
    DatabasesGet,
    DatabasesAll,
    DatabaseSchema,
    DatabaseTypeSchema,
    DatabaseDelete,
    TransactionOpen,
    TransactionClose,
    TransactionCommit,
    TransactionRollback,
    TransactionQuery,
    OneshotQuery,
    // ATTENTION: When adding new Kinds, update all_empty_counts_map()!
}

impl ActionKind {
    fn all_empty_counts_map() -> HashMap<Self, ActionInfo> {
        HashMap::from([
            (Self::ConnectionOpen, ActionInfo::default()),
            (Self::SignIn, ActionInfo::default()),
            (Self::ServersAll, ActionInfo::default()),
            (Self::UsersContains, ActionInfo::default()),
            (Self::UsersCreate, ActionInfo::default()),
            (Self::UsersUpdate, ActionInfo::default()),
            (Self::UsersDelete, ActionInfo::default()),
            (Self::UsersAll, ActionInfo::default()),
            (Self::UsersGet, ActionInfo::default()),
            (Self::Authenticate, ActionInfo::default()),
            (Self::DatabasesContains, ActionInfo::default()),
            (Self::DatabasesCreate, ActionInfo::default()),
            (Self::DatabasesGet, ActionInfo::default()),
            (Self::DatabasesAll, ActionInfo::default()),
            (Self::DatabaseSchema, ActionInfo::default()),
            (Self::DatabaseTypeSchema, ActionInfo::default()),
            (Self::DatabaseDelete, ActionInfo::default()),
            (Self::TransactionOpen, ActionInfo::default()),
            (Self::TransactionClose, ActionInfo::default()),
            (Self::TransactionCommit, ActionInfo::default()),
            (Self::TransactionRollback, ActionInfo::default()),
            (Self::TransactionQuery, ActionInfo::default()),
            (Self::OneshotQuery, ActionInfo::default()),
        ])
    }

    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            ActionKind::ConnectionOpen => "connection_opens",
            ActionKind::SignIn => "sign_ins",
            ActionKind::ServersAll => "server_alls",
            ActionKind::UsersContains => "user_containses",
            ActionKind::UsersCreate => "user_creates",
            ActionKind::UsersUpdate => "user_updates",
            ActionKind::UsersDelete => "user_deletes",
            ActionKind::UsersAll => "user_alls",
            ActionKind::UsersGet => "user_gets",
            ActionKind::Authenticate => "authenticates",
            ActionKind::DatabasesContains => "database_containses",
            ActionKind::DatabasesCreate => "database_creates",
            ActionKind::DatabasesGet => "database_gets",
            ActionKind::DatabasesAll => "database_alls",
            ActionKind::DatabaseSchema => "database_schemas",
            ActionKind::DatabaseTypeSchema => "database_type_schemas",
            ActionKind::DatabaseDelete => "databases_deletes",
            ActionKind::TransactionOpen => "transaction_opens",
            ActionKind::TransactionClose => "transaction_closes",
            ActionKind::TransactionCommit => "transaction_commits",
            ActionKind::TransactionRollback => "transaction_rollbacks",
            ActionKind::TransactionQuery => "transaction_queries",
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

fn get_delta(lhs: u64, rhs: u64) -> i64 {
    if lhs > rhs {
        (lhs - rhs) as i64
    } else {
        -((rhs - lhs) as i64)
    }
}

struct SizeInfo {
    pub total: u64,
    pub available: u64,
}
