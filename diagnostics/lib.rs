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
    sync::{Arc, RwLock},
};

use serde_json::Value as JSONValue;
use xxhash_rust::xxh3::Xxh3;

use crate::{
    metrics::{
        ALL_CLIENT_ENDPOINTS, ActionKind, ClientEndpoint, DatabaseMetricsSnapshot, LoadKind, LoadMetrics, QueryType,
    },
    reports::posthog::{to_full_posthog_reporting_json, to_minimal_posthog_reporting_json},
};

pub mod diagnostics_manager;
pub mod metrics;
mod monitoring_server;
mod reporter;
mod reports;

pub use metrics::TypeDBMetrics;
pub use reports::MonitoringSection;

#[macro_export]
macro_rules! error_with_report {
    ($($arg:tt)+) => {{
        tracing::error!($($arg)+);
        sentry::capture_message(&format!($($arg)+), sentry::Level::Error);
    }};
}

pub(crate) type DatabaseHash = u64;
pub(crate) type DatabaseHashOpt = Option<u64>;

#[derive(Debug, Clone, Eq)]
pub(crate) struct DatabaseId {
    name: Arc<str>,
    hash: DatabaseHash,
}

impl DatabaseId {
    pub(crate) fn new(name: &str) -> Arc<Self> {
        Arc::new(Self { name: Arc::from(name), hash: hash_string_consistently(name) })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn hash_value(&self) -> DatabaseHash {
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
    typedb: TypeDBMetrics,
    monitoring_extensions: RwLock<Vec<Arc<dyn MonitoringSection>>>,

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
            typedb: TypeDBMetrics::new(
                deployment_id,
                server_id,
                distribution,
                version,
                data_directory,
                is_reporting_enabled,
            ),
            monitoring_extensions: RwLock::new(Vec::new()),
            is_full_reporting: is_reporting_enabled,
            metrics_enabled,
        }
    }

    pub fn register_monitoring_extension(&self, source: Arc<dyn MonitoringSection>) {
        let mut exts = self.monitoring_extensions.write().expect("Expected write lock acquisition on extensions");
        let name = source.name().to_string();
        exts.retain(|s| s.name() != name);
        exts.push(source);
    }

    pub fn has_monitoring_extension(&self, name: &str) -> bool {
        let exts = self.monitoring_extensions.read().expect("Expected read lock acquisition on extensions");
        exts.iter().any(|s| s.name() == name)
    }

    pub(crate) fn metrics_enabled(&self) -> bool {
        self.metrics_enabled
    }

    pub(crate) fn typedb(&self) -> &TypeDBMetrics {
        &self.typedb
    }

    pub fn submit_database_metrics(&self, snapshots: HashMap<Arc<str>, DatabaseMetricsSnapshot>) {
        if !self.metrics_enabled {
            return;
        }
        let mut loads = self.typedb.lock_load_metrics_write();
        let mut deleted_databases: HashSet<DatabaseHash> = loads.keys().cloned().collect();

        for (database_name, snapshot) in snapshots {
            let id = DatabaseId::new(database_name.as_ref());
            let database_hash = id.hash_value();
            deleted_databases.remove(&database_hash);

            let database_load = loads.entry(database_hash).or_insert_with(|| LoadMetrics::new(id));
            database_load.set_snapshot(snapshot);
        }

        for database_hash in deleted_databases {
            loads.get_mut(&database_hash).expect("Expected database in load metrics").mark_deleted();
        }
    }

    pub fn increment_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind) {
        if !self.metrics_enabled {
            return;
        }
        let loads = self.typedb.lock_load_metrics_read_for_database(database_name);
        let database_hash = hash_string_consistently(database_name);
        loads.get(&database_hash).expect("Expected database in loads").increment_connection_count(client, load_kind);
    }

    pub fn decrement_load_count(&self, client: ClientEndpoint, database_name: &str, load_kind: LoadKind) {
        if !self.metrics_enabled {
            return;
        }
        // Decrement must have been preceded by increment, so the database is already present.
        let database_hash = hash_string_consistently(database_name);
        let loads = self.typedb.lock_load_metrics_read();
        loads.get(&database_hash).expect("Expected database in loads").decrement_connection_count(client, load_kind);
    }

    pub fn submit_action_success(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let actions = self.typedb.lock_action_metrics_read_for_database(client, database_name, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_success(action_kind);
    }

    pub fn submit_action_fail(&self, client: ClientEndpoint, database_name: Option<&str>, action_kind: ActionKind) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let actions = self.typedb.lock_action_metrics_read_for_database(client, database_name, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_fail(action_kind);
    }

    pub fn submit_error(&self, client: ClientEndpoint, database_name: Option<&str>, error_code: String) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = database_name.map(hash_string_consistently);
        let errors = self.typedb.lock_error_metrics_read_for_database(client, database_name, database_hash);
        errors.get(&database_hash).expect("Expected database in errors").submit(error_code);
    }

    pub fn observe_query_duration(&self, database_name: &str, kind: QueryType, duration: std::time::Duration) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.typedb.lock_histogram_metrics_read_for_database(database_name);
        histograms.get(&database_hash).expect("Expected database in histograms").observe_query_duration(kind, duration);
    }

    pub fn observe_transaction_duration(&self, database_name: &str, kind: LoadKind, duration: std::time::Duration) {
        if !self.metrics_enabled {
            return;
        }
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.typedb.lock_histogram_metrics_read_for_database(database_name);
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
        let histograms = self.typedb.lock_histogram_metrics_read_for_database(database_name);
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
        let histograms = self.typedb.lock_histogram_metrics_read_for_database(database_name);
        histograms
            .get(&database_hash)
            .expect("Expected database in histograms")
            .record_transaction_outcome(kind, outcome);
    }

    pub fn wal_metrics(&self, database_name: &str) -> crate::metrics::FsyncMetrics {
        let database_hash = hash_string_consistently(database_name);
        let histograms = self.typedb.lock_histogram_metrics_read_for_database(database_name);
        let entry = histograms.get(&database_hash).expect("Expected database in histograms");
        entry.wal_metrics()
    }

    pub fn take_snapshot(&self) {
        self.typedb.lock_load_metrics_read().values().for_each(|metrics| metrics.take_snapshot());
        for client in ALL_CLIENT_ENDPOINTS {
            self.typedb.lock_action_metrics_write(client).values_mut().for_each(|metrics| metrics.take_snapshot());
            self.typedb.lock_error_metrics_write(client).values_mut().for_each(|metrics| metrics.take_snapshot());
        }
    }

    pub fn restore_posthog_snapshot(&self) {
        self.typedb.lock_load_metrics_read().values().for_each(|metrics| metrics.restore_snapshot());
        for client in ALL_CLIENT_ENDPOINTS {
            self.typedb.lock_action_metrics_write(client).values_mut().for_each(|metrics| metrics.restore_snapshot());
            self.typedb.lock_error_metrics_write(client).values_mut().for_each(|metrics| metrics.restore_snapshot());
        }
    }

    /// Render all monitoring metrics as a single JSON value.
    ///
    /// The built-in typedb metrics are emitted at the top level. Any registered extensions are
    /// emitted under `extensions.<name>` keyed by `MonitoringSection::name`.
    pub fn to_monitoring_json(&self) -> JSONValue {
        let mut obj = self.typedb.write_json();
        let exts = self.monitoring_extensions.read().expect("Expected read lock acquisition on extensions");
        if !exts.is_empty() {
            let mut ext_map = serde_json::Map::with_capacity(exts.len());
            for ext in exts.iter() {
                ext_map.insert(ext.name().to_string(), JSONValue::Object(ext.write_json()));
            }
            obj.insert("extensions".to_string(), JSONValue::Object(ext_map));
        }
        JSONValue::Object(obj)
    }

    pub fn to_monitoring_prometheus(&self) -> String {
        let mut out = String::new();
        self.typedb.write_prometheus(&mut out);
        let exts = self.monitoring_extensions.read().expect("Expected read lock acquisition on extensions");
        for ext in exts.iter() {
            ext.write_prometheus(&mut out);
        }
        out
    }

    pub fn to_posthog_reporting_json_against_snapshot(&self, api_key: &str) -> JSONValue {
        match self.is_full_reporting {
            true => to_full_posthog_reporting_json(self, api_key),
            false => to_minimal_posthog_reporting_json(self, api_key),
        }
    }
}

// Used when the hash has to be consistent over time and restarts (default hasher does not suit)
pub fn hash_string_consistently(value: impl AsRef<str> + Hash) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(value.as_ref().as_bytes());
    hasher.digest()
}
