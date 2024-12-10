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
        let system_info = System::new_all();
        let os_name = system_info.name();
        let os_arch = system_info.cpu_arch();
        let os_version = system_info.os_version();
        Self { system_info, start_instant: Instant::now(), os_name, os_arch, os_version, version, data_directory }
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
    counts: Mutex<HashMap<Kind, Arc<AtomicU64>>>, // TODO: Should it be mutexed?
    peak_counts: Mutex<HashMap<Kind, Arc<AtomicU64>>>,
}

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ActionKind {
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

#[derive(Debug)]
pub(crate) struct ActionMetrics {
    actions: HashMap<ActionKind, ActionInfo>,
}

#[derive(Debug)]
pub(crate) struct ActionInfo {
    successful: AtomicU64,
    failed: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct ErrorMetrics {
    errors: HashMap<String, ErrorInfo>,
}

#[derive(Debug)]
pub(crate) struct ErrorInfo {
    count: AtomicU64,
}
