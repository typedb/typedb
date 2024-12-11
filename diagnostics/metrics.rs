/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    fmt, fs, io,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Instant,
};

use error::TypeDBError;
use resource::constants::diagnostics::UNKNOWN_STR;
use serde_json::Value as JSONValue;
use sysinfo::System;

use crate::version::JSON_API_VERSION;

trait Metrics {
    fn to_json(&self) -> JSONValue;

    fn to_prometheus_comments(&self) -> String;
    fn to_prometheus_data(&self) -> String;

    // TODO: to_posthog
}

#[derive(Debug)]
pub(crate) struct ServerProperties {
    json_api_version: u64,
    deployment_id: String,
    server_id: String,
    distribution: String,
    reporting_enabled: bool,
}

impl ServerProperties {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        reporting_enabled: bool,
    ) -> ServerProperties {
        Self { json_api_version: JSON_API_VERSION, deployment_id, server_id, distribution, reporting_enabled }
    }
}

#[derive(Debug)]
pub(crate) struct ServerMetrics {
    system_info: System,
    start_instant: Instant,
    os_name: String,
    os_arch: String,
    os_version: String,
    version: String,
    data_directory: PathBuf,
}

impl ServerMetrics {
    pub(crate) fn new(version: String, data_directory: PathBuf) -> ServerMetrics {
        let os_name = System::name().unwrap_or(UNKNOWN_STR.to_owned());
        let os_arch = System::cpu_arch();
        let os_version = System::os_version().unwrap_or(UNKNOWN_STR.to_owned());
        Self {
            system_info: System::new(),
            start_instant: Instant::now(),
            os_name,
            os_arch,
            os_version,
            version,
            data_directory,
        }
    }
}

// TODO: Rename? It's only for internal exchange
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
            // TODO: Maybe optionals?
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
        self.schema = schema;
    }

    pub fn set_data(&mut self, data: DataLoadMetrics) {
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
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SchemaLoadMetrics {
    pub type_count: u64,
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

#[derive(Debug)]
pub(crate) struct ConnectionLoadMetrics {
    counts: HashMap<LoadKind, u64>,
    peak_counts: HashMap<LoadKind, u64>,
}

impl ConnectionLoadMetrics {
    pub fn new() -> Self {
        Self { counts: HashMap::new(), peak_counts: HashMap::new() }
    }

    pub fn increment_count(&mut self, load_kind: LoadKind) {
        let count = self.counts.entry(load_kind).or_insert(0);
        *count += 1;
    }

    pub fn decrement_count(&mut self, load_kind: LoadKind) {
        let count = self.counts.entry(load_kind).or_insert(0);
        assert_ne!(*count, 0u64);
        *count -= 1;
    }

    pub fn take_snapshot(&mut self) {
        todo!()
        // peakCounts.replaceAll((kind, value) -> new AtomicLong(counts.get(kind).get()));
    }
}

#[derive(Debug)]
pub(crate) struct ActionMetrics {
    actions: HashMap<ActionKind, ActionInfo>,
}

impl ActionMetrics {
    pub fn new() -> Self {
        Self { actions: HashMap::new() }
    }

    pub fn submit_success(&mut self, action_kind: ActionKind) {
        self.actions.entry(action_kind).or_insert(ActionInfo::new()).submit_success();
    }
    pub fn submit_fail(&mut self, action_kind: ActionKind) {
        self.actions.entry(action_kind).or_insert(ActionInfo::new()).submit_fail();
    }

    pub fn take_snapshot(&mut self) {
        todo!()
        // requestInfosSnapshot.clear();
        // requestInfos.forEach((kind, requestInfo) -> requestInfosSnapshot.put(kind, requestInfo.clone()));
    }
}

#[derive(Debug)]
pub(crate) struct ActionInfo {
    successful: u64,
    failed: u64,
}

impl ActionInfo {
    pub fn new() -> Self {
        Self { successful: 0, failed: 0 }
    }

    pub fn submit_success(&mut self) {
        self.successful += 1;
    }

    pub fn submit_fail(&mut self) {
        self.failed += 1;
    }

    pub fn take_snapshot(&mut self) {
        todo!()
        // requestInfosSnapshot.clear(); ???
        // requestInfos.forEach((kind, requestInfo) -> requestInfosSnapshot.put(kind, requestInfo.clone()));
    }
}

#[derive(Debug)]
pub(crate) struct ErrorMetrics {
    errors: HashMap<String, ErrorInfo>,
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self { errors: HashMap::new() }
    }

    pub fn submit(&mut self, error_code: String) {
        self.errors.entry(error_code).or_insert(ErrorInfo::new()).submit();
    }

    pub fn take_snapshot(&mut self) {
        todo!()
        // errorCountsSnapshot.clear();
        // errorCounts.forEach((code, count) -> errorCountsSnapshot.put(code, new AtomicLong(count.get())));
    }
}

#[derive(Debug)]
pub(crate) struct ErrorInfo {
    count: u64,
}

impl ErrorInfo {
    pub fn new() -> Self {
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

impl fmt::Display for LoadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: I guess we want to support all the possible 2.x names?
        match self {
            LoadKind::SchemaTransactions => write!(f, "SCHEMA_TRANSACTIONS"),
            LoadKind::ReadTransactions => write!(f, "READ_TRANSACTIONS"),
            LoadKind::WriteTransactions => write!(f, "WRITE_TRANSACTIONS"),
        }
    }
}

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub enum ActionKind {
    ConnectionOpen,
    ServersAll,
    UsersContains,
    UsersCreate,
    UsersDelete,
    UsersAll,
    UsersGet,
    UsersPasswordSet,
    UserPasswordUpdate,
    UserToken,
    DatabasesContains,
    DatabasesCreate,
    DatabasesGet,
    DatabasesAll,
    DatabaseSchema,
    DatabaseTypeSchema,
    DatabaseDelete,
    TransactionOpen,
    TransactionClose,
    TransactionExecute, // TODO: Split to commit, rollback, etc?
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: I guess we want to support all the possible 2.x names?
        match self {
            ActionKind::ConnectionOpen => write!(f, "CONNECTION_OPEN"),
            ActionKind::ServersAll => write!(f, "SERVERS_ALL"),
            ActionKind::UsersContains => write!(f, "USERS_CONTAINS"),
            ActionKind::UsersCreate => write!(f, "USERS_CREATE"),
            ActionKind::UsersDelete => write!(f, "USERS_DELETE"),
            ActionKind::UsersAll => write!(f, "USERS_ALL"),
            ActionKind::UsersGet => write!(f, "USERS_GET"),
            ActionKind::UsersPasswordSet => write!(f, "USERS_PASSWORD_SET"),
            ActionKind::UserPasswordUpdate => write!(f, "USER_PASSWORD_UPDATE"),
            ActionKind::UserToken => write!(f, "USER_TOKEN"),
            ActionKind::DatabasesContains => write!(f, "DATABASES_CONTAINS"),
            ActionKind::DatabasesCreate => write!(f, "DATABASES_CREATE"),
            ActionKind::DatabasesGet => write!(f, "DATABASES_GET"),
            ActionKind::DatabasesAll => write!(f, "DATABASES_ALL"),
            ActionKind::DatabaseSchema => write!(f, "DATABASES_SCHEMA"),
            ActionKind::DatabaseTypeSchema => write!(f, "DATABASES_TYPE_SCHEMA"),
            ActionKind::DatabaseDelete => write!(f, "DATABASES_DELETE"),
            ActionKind::TransactionOpen => write!(f, "TRANSACTION_OPEN"),
            ActionKind::TransactionClose => write!(f, "TRANSACTION_CLOSE"),
            ActionKind::TransactionExecute => write!(f, "TRANSACTION_EXECUTE"),
        }
    }
}
