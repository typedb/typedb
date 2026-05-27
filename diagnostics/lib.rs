/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use serde_json::Value as JSONValue;
use xxhash_rust::xxh3::Xxh3;

use crate::{
    metrics::{
        ALL_CLIENT_ENDPOINTS, ActionKind, ActionMetrics, ClientEndpoint, DatabaseHistograms, DatabaseMetrics,
        ErrorMetrics, LoadKind, LoadMetrics, QueryType, ServerMetrics, ServerProperties, client_endpoints_map,
    },
    reports::{
        json_monitoring::to_monitoring_json,
        posthog::{to_full_posthog_reporting_json, to_minimal_posthog_reporting_json},
        prometheus_monitoring::to_monitoring_prometheus,
    },
};

pub mod diagnostics_manager;
pub mod metrics;
mod monitoring_server;
mod reporter;
mod reports;

#[macro_export]
macro_rules! error_with_report {
    ($($arg:tt)+) => {{
        tracing::error!($($arg)+);
        sentry::capture_message(&format!($($arg)+), sentry::Level::Error);
    }};
}

type DatabaseHash = u64;
type DatabaseHashOpt = Option<u64>;

/// Carries both the human name (for the local Prometheus `database` label) and
/// the xxh3 hash (the only identifier Posthog ever sees). PII discipline is
/// enforced at the type: `DatabaseReport` serializes only the hash.
#[derive(Debug, Clone, Eq)]
pub(crate) struct DatabaseId {
    name: Arc<str>,
    hash: u64,
}

impl DatabaseId {
    pub(crate) fn new(name: &str) -> Arc<Self> {
        Arc::new(Self { name: Arc::from(name), hash: hash_string_consistently(name) })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn hash_value(&self) -> u64 {
        self.hash
    }
}

impl PartialEq for DatabaseId {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Hash for DatabaseId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug)]
pub struct Diagnostics {
    server_properties: ServerProperties,
    server_metrics: ServerMetrics,
    load_metrics: RwLock<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ActionMetrics>>>,
    error_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ErrorMetrics>>>,
    histogram_metrics: RwLock<HashMap<DatabaseHash, DatabaseHistograms>>,

    is_full_reporting: bool,
    metrics_enabled: bool,
}

impl Diagnostics {
    pub fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        is_reporting_enabled: bool,
        metrics_enabled: bool,
    ) -> Diagnostics {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, is_reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: RwLock::new(HashMap::new()),
            action_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            error_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            histogram_metrics: RwLock::new(HashMap::new()),

            is_full_reporting: is_reporting_enabled,
            metrics_enabled,
        }
    }

    pub(crate) fn metrics_enabled(&self) -> bool {
        self.metrics_enabled
    }

    pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>) {
        if !self.metrics_enabled {
            return;
        }
        let mut loads = self.lock_load_metrics_write();
        let mut deleted_databases: HashSet<DatabaseHash> = loads.keys().cloned().collect();

        for metrics in database_metrics {
            let id = DatabaseId::new(metrics.database_name.as_ref());
            let database_hash = id.hash_value();
            deleted_databases.remove(&database_hash);

            let database_load = loads.entry(database_hash).or_insert_with(|| LoadMetrics::new(id));
            database_load.set_schema(metrics.schema);
            database_load.set_data(metrics.data);
        }

        for database_hash in deleted_databases {
            loads.get_mut(&database_hash).expect("Expected database in load metrics").mark_deleted();
        }
    }

    pub fn increment_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind) {
        if !self.metrics_enabled {
            return;
        }
        let loads = self.lock_load_metrics_read_for_database(database_name);
        let database_hash = hash_string_consistently(database_name);
        loads.get(&database_hash).expect("Expected database in loads").increment_connection_count(client, load_kind);
    }

    pub fn decrement_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind) {
        if !self.metrics_enabled {
            return;
        }
        // Decrement must have been preceded by increment, so the database is already present.
        let database_hash = hash_string_consistently(database_name);
        let loads = self.lock_load_metrics_read();
        loads.get(&database_hash).expect("Expected database in loads").decrement_connection_count(client, load_kind);
    }

    pub fn submit_action_success(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let actions = self.lock_action_metrics_read_for_database(client, database_name, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_success(action_kind);
    }

    pub fn submit_action_fail(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let actions = self.lock_action_metrics_read_for_database(client, database_name, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_fail(action_kind);
    }

    pub fn submit_error(&self, client: ClientEndpoint, database_name: Option<&str>, error_code: String) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let errors = self.lock_error_metrics_read_for_database(client, database_name, database_hash);
        errors.get(&database_hash).expect("Expected database in errors").submit(error_code);
    }

    pub fn observe_query_duration(&self, database_name: &str, kind: QueryType, duration: std::time::Duration) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.lock_histogram_metrics_read_for_database(database_name);
        histograms.get(&database_hash).expect("Expected database in histograms").observe_query_duration(kind, duration);
    }

    pub fn observe_transaction_duration(&self, database_name: &str, kind: LoadKind, duration: std::time::Duration) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.lock_histogram_metrics_read_for_database(database_name);
        histograms
            .get(&database_hash)
            .expect("Expected database in histograms")
            .observe_transaction_duration(kind, duration);
    }

    pub fn observe_queries_per_transaction(&self, database_name: &str, queries: u64) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.lock_histogram_metrics_read_for_database(database_name);
        histograms
            .get(&database_hash)
            .expect("Expected database in histograms")
            .observe_queries_per_transaction(queries);
    }

    pub fn record_transaction_outcome(
        &self,
        database_name: &str,
        kind: LoadKind,
        outcome: crate::metrics::TransactionOutcome,
    ) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.lock_histogram_metrics_read_for_database(database_name);
        histograms
            .get(&database_hash)
            .expect("Expected database in histograms")
            .record_transaction_outcome(kind, outcome);
    }

    pub fn wal_metrics(&self, database_name: &str) -> crate::metrics::FsyncMetrics {
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.lock_histogram_metrics_read_for_database(database_name);
        let entry = histograms.get(&database_hash).expect("Expected database in histograms");
        entry.wal_metrics()
    }

    pub(crate) fn histogram_snapshots(&self) -> Vec<(Arc<DatabaseId>, crate::metrics::DatabaseHistogramsSnapshot)> {
        self.lock_histogram_metrics_read().values().map(|db| (db.database_id().clone(), db.snapshot())).collect()
    }

    pub fn take_snapshot(&self) {
        self.lock_load_metrics_read().values().for_each(|metrics| metrics.take_snapshot());
        for client in ALL_CLIENT_ENDPOINTS {
            self.lock_action_metrics_write(client).values_mut().for_each(|metrics| metrics.take_snapshot());
            self.lock_error_metrics_write(client).values_mut().for_each(|metrics| metrics.take_snapshot());
        }
    }

    pub fn restore_posthog_snapshot(&self) {
        self.lock_load_metrics_read().values().for_each(|metrics| metrics.restore_snapshot());
        for client in ALL_CLIENT_ENDPOINTS {
            self.lock_action_metrics_write(client).values_mut().for_each(|metrics| metrics.restore_snapshot());
            self.lock_error_metrics_write(client).values_mut().for_each(|metrics| metrics.restore_snapshot());
        }
    }

    pub fn to_monitoring_json(&self) -> JSONValue {
        to_monitoring_json(self)
    }

    pub fn to_monitoring_prometheus(&self) -> String {
        to_monitoring_prometheus(self)
    }

    pub fn to_posthog_reporting_json_against_snapshot(&self, api_key: &str) -> JSONValue {
        match self.is_full_reporting {
            true => to_full_posthog_reporting_json(self, api_key),
            false => to_minimal_posthog_reporting_json(self, api_key),
        }
    }
}

impl Diagnostics {
    fn lock_load_metrics_read_for_database(
        &self,
        database_name: &str,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        let database_hash = hash_string_consistently(database_name);
        if let Some(lock) = self.try_lock_load_metrics_read_for_database(database_hash) {
            return lock;
        }
        self.add_database_to_load_metrics(DatabaseId::new(database_name));
        self.try_lock_load_metrics_read_for_database(database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_load_metrics_read_for_database(
        &self,
        database_hash: DatabaseHash,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>>> {
        let read_lock = self.lock_load_metrics_read();
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_load_metrics(&self, id: Arc<DatabaseId>) {
        let mut write_lock = self.lock_load_metrics_write();
        let database_hash = id.hash_value();
        if !write_lock.contains_key(&database_hash) {
            write_lock.insert(database_hash, LoadMetrics::new(id));
        }
    }

    fn lock_load_metrics_read(&self) -> RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.read().expect("Expected read lock acquisition")
    }

    fn lock_load_metrics_write(&self) -> RwLockWriteGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.write().expect("Expected write lock acquisition")
    }

    fn lock_histogram_metrics_read_for_database(
        &self,
        database_name: &str,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        let database_hash = hash_string_consistently(database_name);
        if let Some(lock) = self.try_lock_histogram_metrics_read_for_database(database_hash) {
            return lock;
        }
        self.add_database_to_histogram_metrics(DatabaseId::new(database_name));
        self.try_lock_histogram_metrics_read_for_database(database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_histogram_metrics_read_for_database(
        &self,
        database_hash: DatabaseHash,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>>> {
        let read_lock = self.lock_histogram_metrics_read();
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_histogram_metrics(&self, id: Arc<DatabaseId>) {
        let mut write_lock = self.lock_histogram_metrics_write();
        let database_hash = id.hash_value();
        if !write_lock.contains_key(&database_hash) {
            write_lock.insert(database_hash, DatabaseHistograms::new(id));
        }
    }

    pub(crate) fn lock_histogram_metrics_read(&self) -> RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        self.histogram_metrics.read().expect("Expected read lock acquisition")
    }

    fn lock_histogram_metrics_write(&self) -> RwLockWriteGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        self.histogram_metrics.write().expect("Expected write lock acquisition")
    }

    fn lock_action_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_name: Option<&str>,
        database_hash: DatabaseHashOpt,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        if let Some(lock) = self.try_lock_action_metrics_read_for_database(client, database_hash) {
            return lock;
        }
        self.add_database_to_action_metrics(client, database_name);
        self.try_lock_action_metrics_read_for_database(client, database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_action_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_hash: DatabaseHashOpt,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>>> {
        let read_lock = self.lock_action_metrics_read(client);
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_action_metrics(&self, client: ClientEndpoint, database_name: Option<&str>) {
        let id = database_name.map(DatabaseId::new);
        let database_hash = id.as_ref().map(|id| id.hash_value());
        let mut write_lock = self.lock_action_metrics_write(client);
        if !write_lock.contains_key(&database_hash) {
            write_lock.insert(database_hash, ActionMetrics::new(id));
        }
    }

    pub(crate) fn lock_action_metrics_read(
        &self,
        client: ClientEndpoint,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics
            .get(&client)
            .expect("Expected client {client}")
            .read()
            .expect("Expected read lock acquisition")
    }

    fn lock_action_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }

    fn lock_error_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_name: Option<&str>,
        database_hash: DatabaseHashOpt,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        if let Some(lock) = self.try_lock_error_metrics_read_for_database(client, database_hash) {
            return lock;
        }
        self.add_database_to_error_metrics(client, database_name);
        self.try_lock_error_metrics_read_for_database(client, database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_error_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_hash: DatabaseHashOpt,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>>> {
        let read_lock = self.lock_error_metrics_read(client);
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_error_metrics(&self, client: ClientEndpoint, database_name: Option<&str>) {
        let id = database_name.map(DatabaseId::new);
        let database_hash = id.as_ref().map(|id| id.hash_value());
        let mut write_lock = self.lock_error_metrics_write(client);
        if !write_lock.contains_key(&database_hash) {
            write_lock.insert(database_hash, ErrorMetrics::new(id));
        }
    }

    pub(crate) fn lock_error_metrics_read(
        &self,
        client: ClientEndpoint,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics
            .get(&client)
            .expect("Expected client {client}")
            .read()
            .expect("Expected read lock acquisition")
    }

    fn lock_error_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }
}

// Used when the hash has to be consistent over time and restarts (default hasher does not suit)
pub fn hash_string_consistently(value: impl AsRef<str> + Hash) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(value.as_ref().as_bytes());
    hasher.digest()
}
