/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, path::PathBuf, time::Instant};

use chrono::Utc;
use resource::constants::diagnostics::{REPORT_INTERVAL, UNKNOWN_STR};
use serde_json::{json, map::Map as JSONMap, Value as JSONValue};
use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System};

use crate::{version::JSON_API_VERSION, DatabaseHash, DatabaseHashOpt};

#[derive(Debug)]
pub(crate) struct ServerProperties {
    json_api_version: u64,
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
        Self { json_api_version: JSON_API_VERSION, deployment_id, server_id, distribution, is_reporting_enabled }
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

    pub fn to_reporting_json(&self) -> JSONValue {
        json!({
            "version": self.json_api_version,
            "deploymentID": self.deployment_id,
            "serverID": self.server_id,
            "distribution": self.distribution,
            "timestamp": self.format_datetime(Utc::now()),
            "periodInSeconds": REPORT_INTERVAL.as_secs(),
            "enabled": self.is_reporting_enabled
        })
    }

    pub fn to_monitoring_json(&self) -> JSONValue {
        json!({
            "version": self.json_api_version,
            "deploymentID": self.deployment_id,
            "serverID": self.server_id,
            "distribution": self.distribution,
            "timestamp": self.format_datetime(Utc::now())
        })
    }

    pub fn to_prometheus_comment(&self) -> String {
        format!("# distribution: {}\n", self.distribution)
    }

    fn format_datetime(&self, datetime: chrono::DateTime<Utc>) -> String {
        datetime.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
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

    pub fn to_reporting_minimal_json(&self) -> JSONValue {
        json!({
            "version": self.version
        })
    }

    pub fn to_json(&self) -> JSONValue {
        let memory_info = self.get_memory_info();
        let disk_info = self.get_disk_info();

        json!({
            "version": self.version,
            "uptimeInSeconds": self.get_uptime_in_seconds(),
            "os": {
                "name": self.os_name,
                "arch": self.os_arch,
                "version": self.os_version
            },
            "memoryUsedInBytes": memory_info.total - memory_info.available,
            "memoryAvailableInBytes": memory_info.available,
            "diskUsedInBytes": disk_info.total - disk_info.available,
            "diskAvailableInBytes": disk_info.available,
        })
    }

    pub fn prometheus_header() -> &'static str {
        "# TYPE server_resources_count gauge"
    }

    pub fn to_prometheus_comment(&self) -> String {
        format!("# version: {}\n# os: {} {} {}\n", self.version, self.os_name, self.os_arch, self.os_version)
    }

    pub fn to_prometheus_data(&self) -> String {
        let header = "server_resources_count";
        let memory_info = self.get_memory_info();
        let disk_info = self.get_disk_info();

        format!(
            "{header}{{kind=\"memoryUsedInBytes\"}} {}\n\
             {header}{{kind=\"memoryAvailableInBytes\"}} {}\n\
             {header}{{kind=\"diskUsedInBytes\"}} {}\n\
             {header}{{kind=\"diskAvailableInBytes\"}} {}\n",
            memory_info.total - memory_info.available,
            memory_info.available,
            disk_info.total - disk_info.available,
            disk_info.available,
            header = header
        )
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
            Err(_) => None, // TODO: Ignore?
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

    pub fn increment_connection_count(&mut self, load_kind: LoadKind) {
        self.connection.increment_count(load_kind);
    }

    pub fn decrement_connection_count(&mut self, load_kind: LoadKind) {
        self.connection.decrement_count(load_kind);
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    pub fn take_snapshot(&mut self) {
        self.connection.take_snapshot()
    }

    pub fn to_reporting_json(&self, database_hash: &DatabaseHash, is_owned: bool) -> Option<JSONValue> {
        if !self.is_deleted || !self.connection.is_empty() {
            let mut load_object = json!({
                "database": format!("{:.0}", database_hash),
                "connection": self.connection.to_json(),
            });

            if is_owned {
                load_object.as_object_mut().unwrap().insert("schema".to_string(), self.schema.to_json());
                load_object.as_object_mut().unwrap().insert("data".to_string(), self.data.to_json());
            }

            Some(load_object)
        } else {
            None
        }
    }

    pub fn to_monitoring_json(&self, database_hash: &DatabaseHash, is_owned: bool) -> Option<JSONValue> {
        if is_owned && !self.is_deleted {
            let load_object = json!({
                "database": format!("{:.0}", database_hash),
                "schema": self.schema.to_json(),
                "data": self.data.to_json(),
            });
            Some(load_object)
        } else {
            None
        }
    }

    pub fn prometheus_header() -> &'static str {
        "# TYPE typedb_schema_data_count gauge"
    }

    pub fn to_prometheus_data(&self, database_hash: &DatabaseHash, is_primary_server: bool) -> String {
        if is_primary_server && !self.is_deleted {
            format!("{}{}", self.schema.to_prometheus_data(database_hash), self.data.to_prometheus_data(database_hash))
        } else {
            String::new()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SchemaLoadMetrics {
    pub type_count: u64,
}

impl SchemaLoadMetrics {
    pub fn to_json(&self) -> JSONValue {
        json!({ "typeCount": self.type_count })
    }

    pub fn to_prometheus_data(&self, database_hash: &DatabaseHash) -> String {
        format!("typedb_schema_data_count{{database=\"{}\", kind=\"typeCount\"}} {}\n", database_hash, self.type_count)
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
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "entityCount": self.entity_count,
            "relationCount": self.relation_count,
            "attributeCount": self.attribute_count,
            "hasCount": self.has_count,
            "roleCount": self.role_count,
            "storageInBytes": self.storage_in_bytes,
            "storageKeyCount": self.storage_key_count
        })
    }

    pub fn to_prometheus_data(&self, database_hash: &DatabaseHash) -> String {
        let header = format!("typedb_schema_data_count{{database=\"{}\", kind=", database_hash);
        format!(
            "{}\"entityCount\"}} {}\n{}\"relationCount\"}} {}\n{}\"attributeCount\"}} {}\n{}\"hasCount\"}} {}\n{}\"roleCount\"}} {}\n{}\"storageInBytes\"}} {}\n{}\"storageKeyCount\"}} {}\n",
            header, self.entity_count,
            header, self.relation_count,
            header, self.attribute_count,
            header, self.has_count,
            header, self.role_count,
            header, self.storage_in_bytes,
            header, self.storage_key_count
        )
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionLoadMetrics {
    counts: HashMap<LoadKind, u64>,
    peak_counts: HashMap<LoadKind, u64>,
}

impl ConnectionLoadMetrics {
    pub fn new() -> Self {
        Self { counts: LoadKind::all_empty_counts_map(), peak_counts: LoadKind::all_empty_counts_map() }
    }

    pub fn increment_count(&mut self, load_kind: LoadKind) {
        let new_count = {
            let count = self.counts.entry(load_kind).or_insert(0);
            *count += 1;
            *count
        };
        self.update_peak_counts(load_kind, new_count);
    }

    pub fn decrement_count(&mut self, load_kind: LoadKind) {
        let count = self.counts.entry(load_kind).or_insert(0);
        assert_ne!(*count, 0u64);
        *count -= 1;
    }

    pub fn update_peak_counts(&mut self, load_kind: LoadKind, count: u64) {
        if self.peak_counts.get(&load_kind).unwrap_or(&0u64) < &count {
            self.peak_counts.insert(load_kind, count);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.peak_counts.values().sum::<u64>() == 0u64
    }

    pub fn take_snapshot(&mut self) {
        let kinds: Vec<_> = self.peak_counts.keys().cloned().collect();
        for kind in kinds {
            self.peak_counts.insert(kind, *self.counts.get(&kind).expect("Expected count"));
        }
    }

    pub fn to_json(&self) -> JSONValue {
        let mut peak = JSONMap::new();
        for (kind, &value) in self.peak_counts.iter() {
            peak.insert(kind.to_string(), json!(value));
        }
        json!(peak)
    }
}

#[derive(Debug)]
pub(crate) struct ActionMetrics {
    actions: HashMap<ActionKind, ActionInfo>,
    actions_snapshot: HashMap<ActionKind, ActionInfo>,
}

impl ActionMetrics {
    pub fn new() -> Self {
        Self { actions: HashMap::new(), actions_snapshot: HashMap::new() }
    }

    pub fn submit_success(&mut self, action_kind: ActionKind) {
        self.actions.entry(action_kind).or_insert(ActionInfo::new()).submit_success();
    }
    pub fn submit_fail(&mut self, action_kind: ActionKind) {
        self.actions.entry(action_kind).or_insert(ActionInfo::new()).submit_fail();
    }

    fn get_successful_delta(&self, action_kind: &ActionKind) -> i64 {
        get_delta(
            self.actions.get(action_kind).unwrap_or(&ActionInfo::default()).successful,
            self.actions_snapshot.get(action_kind).unwrap_or(&ActionInfo::default()).successful,
        )
    }

    fn get_failed_delta(&self, action_kind: &ActionKind) -> i64 {
        get_delta(
            self.actions.get(action_kind).unwrap_or(&ActionInfo::default()).failed,
            self.actions_snapshot.get(action_kind).unwrap_or(&ActionInfo::default()).failed,
        )
    }

    pub fn take_snapshot(&mut self) {
        for kind in self.actions.keys() {
            *self.actions_snapshot.entry(*kind).or_insert(ActionInfo::default()) =
                self.actions.get(kind).expect("Expected action by kind").clone();
        }
    }

    pub fn to_reporting_json(&self, database_hash: &DatabaseHashOpt) -> Vec<JSONValue> {
        let mut actions = vec![];

        for kind in self.actions.keys() {
            let successful = self.get_successful_delta(kind);
            let failed = self.get_failed_delta(kind);
            if successful == 0 && failed == 0 {
                continue;
            }

            let mut object = JSONMap::new();
            object.insert("name".to_string(), json!(kind.to_string()));
            if let Some(database_hash) = database_hash {
                object.insert("database".to_string(), json!(format!("{:.0}", database_hash)));
            }
            object.insert("successful".to_string(), json!(successful));
            object.insert("failed".to_string(), json!(failed));
            actions.push(json!(object));
        }

        actions
    }

    pub fn to_monitoring_json(&self, database_hash: &DatabaseHashOpt) -> Vec<JSONValue> {
        let mut actions = vec![];

        for (kind, info) in &self.actions {
            let mut request_object = serde_json::Map::new();
            request_object.insert("name".to_string(), json!(kind.to_string()));
            if let Some(database_hash) = database_hash {
                request_object.insert("database".to_string(), json!(format!("{:.0}", database_hash)));
            }
            request_object.insert("attempted".to_string(), json!(info.get_attempted()));
            request_object.insert("successful".to_string(), json!(info.get_successful()));
            actions.push(json!(request_object));
        }

        actions
    }

    pub fn prometheus_header_attempted() -> &'static str {
        "# TYPE typedb_attempted_requests_total counter"
    }

    pub fn prometheus_header_successful() -> &'static str {
        "# TYPE typedb_successful_requests_total counter"
    }

    pub fn to_prometheus_data_attempted(&self, database_hash: &DatabaseHashOpt) -> String {
        let mut buf = String::new();
        for (kind, info) in &self.actions {
            buf.push_str("typedb_attempted_requests_total{");
            if let Some(database_hash) = database_hash {
                buf.push_str(&format!("database=\"{}\", ", database_hash));
            }
            buf.push_str(&format!("kind=\"{}\"}} {}\n", kind, info.get_attempted()));
        }
        buf
    }

    pub fn to_prometheus_data_successful(&self, database_hash: &DatabaseHashOpt) -> String {
        let mut buf = String::new();
        for (kind, info) in &self.actions {
            buf.push_str("typedb_successful_requests_total{");
            if let Some(database_hash) = database_hash {
                buf.push_str(&format!("database=\"{}\", ", database_hash));
            }
            buf.push_str(&format!("kind=\"{}\"}} {}\n", kind, info.get_successful()));
        }
        buf
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ActionInfo {
    successful: u64,
    failed: u64,
}

impl ActionInfo {
    pub const fn new() -> Self {
        Self::default()
    }

    pub const fn default() -> Self {
        Self { successful: 0, failed: 0 }
    }

    pub fn submit_success(&mut self) {
        self.successful += 1;
    }

    pub fn submit_fail(&mut self) {
        self.failed += 1;
    }

    pub fn get_successful(&self) -> u64 {
        self.successful
    }

    pub fn get_failed(&self) -> u64 {
        self.failed
    }

    pub fn get_attempted(&self) -> u64 {
        self.successful + self.failed
    }
}

#[derive(Debug)]
pub(crate) struct ErrorMetrics {
    errors: HashMap<String, ErrorInfo>,
    errors_snapshot: HashMap<String, ErrorInfo>,
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self { errors: HashMap::new(), errors_snapshot: HashMap::new() }
    }

    pub fn submit(&mut self, error_code: String) {
        self.errors.entry(error_code).or_insert(ErrorInfo::new()).submit();
    }

    fn get_count_delta(&self, error_code: &str) -> i64 {
        get_delta(
            self.errors.get(error_code).unwrap_or(&ErrorInfo::default()).count,
            self.errors_snapshot.get(error_code).unwrap_or(&ErrorInfo::default()).count,
        )
    }

    pub fn take_snapshot(&mut self) {
        for code in self.errors.keys() {
            *self.errors_snapshot.entry(code.clone()).or_insert(ErrorInfo::default()) =
                self.errors.get(code).expect("Expected error by code").clone();
        }
    }

    pub fn to_reporting_json(&self, database_hash: &DatabaseHashOpt) -> Vec<JSONValue> {
        let mut errors = vec![];

        for code in self.errors.keys() {
            let count_delta = self.get_count_delta(code);
            if count_delta == 0 {
                continue;
            }

            let mut error_object = JSONMap::new();
            error_object.insert("code".to_string(), json!(code));
            if let Some(database_hash) = database_hash {
                error_object.insert("database".to_string(), json!(format!("{:.0}", database_hash)));
            }
            error_object.insert("count".to_string(), json!(count_delta));

            errors.push(JSONValue::Object(error_object));
        }

        errors
    }

    pub fn to_monitoring_json(&self, database_hash: &DatabaseHashOpt) -> Vec<JSONValue> {
        let mut errors = vec![];

        for (code, info) in &self.errors {
            let mut error_object = JSONMap::new();
            error_object.insert("code".to_string(), json!(code));
            if let Some(database_hash) = database_hash {
                error_object.insert("database".to_string(), json!(format!("{:.0}", database_hash)));
            }
            error_object.insert("count".to_string(), json!(info.count));

            errors.push(JSONValue::Object(error_object));
        }

        errors
    }

    pub fn prometheus_header() -> &'static str {
        "# TYPE typedb_error_total counter"
    }

    pub fn to_prometheus_data(&self, database_hash: &DatabaseHashOpt) -> String {
        let mut buf = String::new();
        for (code, info) in &self.errors {
            buf.push_str("typedb_error_total{");

            if let Some(database_hash) = database_hash {
                buf.push_str(&format!("database=\"{}\", ", database_hash));
            }
            buf.push_str(&format!("code=\"{}\"}} {} \n", code, info.count));
        }
        buf
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
}

// TODO: Would be great to have a static protection that every kind is covered here
impl LoadKind {
    const ALL_EMPTY_COUNTS: [(LoadKind, u64); 3] =
        [(LoadKind::SchemaTransactions, 0u64), (LoadKind::WriteTransactions, 0u64), (LoadKind::ReadTransactions, 0u64)];

    fn all_empty_counts_map() -> HashMap<LoadKind, u64> {
        HashMap::from(Self::ALL_EMPTY_COUNTS)
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

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub enum ActionKind {
    ConnectionOpen,
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
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActionKind::ConnectionOpen => write!(f, "CONNECTION_OPEN"),
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
