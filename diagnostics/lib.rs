/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use error::TypeDBError;
use serde_json::Value as JSONValue;
use xxhash_rust::xxh3::Xxh3;

use crate::metrics::{
    ActionKind, ActionMetrics, DatabaseMetrics, ErrorMetrics, LoadKind, LoadMetrics, ServerMetrics, ServerProperties,
};

pub mod diagnostics_manager;
pub mod metrics;
mod monitoring_server;
mod reporter;
mod version;

type DatabaseHash = u64;
type DatabaseHashOpt = Option<u64>;

#[derive(Debug)]
pub struct Diagnostics {
    server_properties: ServerProperties,
    server_metrics: ServerMetrics,
    load_metrics: Mutex<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: Mutex<HashMap<DatabaseHashOpt, ActionMetrics>>,
    error_metrics: Mutex<HashMap<DatabaseHashOpt, ErrorMetrics>>,

    is_full_reporting: bool,
    owned_databases: Mutex<HashSet<DatabaseHash>>,
}

impl Diagnostics {
    pub fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        is_reporting_enabled: bool,
    ) -> Diagnostics {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, is_reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: Mutex::new(HashMap::new()),
            action_metrics: Mutex::new(HashMap::new()),
            error_metrics: Mutex::new(HashMap::new()),

            is_full_reporting: is_reporting_enabled,
            owned_databases: Mutex::new(HashSet::new()),
        }
    }

    pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>) {
        let mut loads = self.lock_load_metrics();
        let mut deleted_databases: HashSet<DatabaseHash> = loads.keys().cloned().collect();

        for metrics in database_metrics {
            let database_hash = Self::hash_database(metrics.database_name);
            deleted_databases.remove(&database_hash);

            let database_load = loads.entry(database_hash).or_insert(LoadMetrics::new());
            database_load.set_schema(metrics.schema);
            database_load.set_data(metrics.data);

            self.update_owned_databases(database_hash, metrics.is_primary_server);
        }

        for database_hash in deleted_databases {
            loads.get_mut(&database_hash).expect("Expected database in load metrics").mark_deleted();
        }
    }

    pub fn increment_load_count(&self, database_name: &str, load_kind: LoadKind) {
        let database_hash = Self::hash_database(database_name);
        let mut loads = self.lock_load_metrics();
        loads.entry(database_hash).or_insert(LoadMetrics::new()).increment_connection_count(load_kind);
    }

    pub fn decrement_load_count(&self, database_name: &str, load_kind: LoadKind) {
        let database_hash = Self::hash_database(database_name);
        let mut loads = self.lock_load_metrics();
        loads.entry(database_hash).or_insert(LoadMetrics::new()).decrement_connection_count(load_kind);
    }

    pub fn submit_action_success(&self, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind) {
        let database_hash = Self::hash_database_opt(database_name);
        let mut actions = self.lock_action_metrics();
        actions.entry(database_hash).or_insert(ActionMetrics::new()).submit_success(action_kind);
    }

    pub fn submit_action_fail(&self, database_name: Option<impl AsRef<str> + Hash>, action_kind: ActionKind) {
        let database_hash = Self::hash_database_opt(database_name);
        let mut actions = self.lock_action_metrics();
        actions.entry(database_hash).or_insert(ActionMetrics::new()).submit_fail(action_kind);
    }

    pub fn submit_error(&self, database_name: Option<impl AsRef<str> + Hash>, error_code: String) {
        let database_hash = Self::hash_database_opt(database_name);
        let mut errors = self.lock_error_metrics();
        errors.entry(database_hash).or_insert(ErrorMetrics::new()).submit(error_code);
    }

    pub fn take_snapshot(&self) {
        self.error_metrics
            .lock()
            .expect("Expected error metrics lock acquisition")
            .values_mut()
            .for_each(|metrics| metrics.take_snapshot());
        self.action_metrics
            .lock()
            .expect("Expected action metrics lock acquisition")
            .values_mut()
            .for_each(|metrics| metrics.take_snapshot());
        self.load_metrics
            .lock()
            .expect("Expected load metrics lock acquisition")
            .values_mut()
            .for_each(|metrics| metrics.take_snapshot());
    }

    pub fn to_reporting_json_against_snapshot(&self) -> JSONValue {
        match self.is_full_reporting {
            true => self.to_full_reporting_json(),
            false => self.to_minimal_reporting_json(),
        }
    }

    fn to_full_reporting_json(&self) -> JSONValue {
        let mut diagnostics = self.server_properties.to_reporting_json();

        diagnostics["server"] = self.server_metrics.to_full_reporting_json();

        let load = self
            .lock_load_metrics()
            .iter()
            .filter_map(|(database_hash, metrics)| {
                metrics.to_reporting_json(database_hash, self.is_owned(database_hash))
            })
            .collect();
        diagnostics["load"] = JSONValue::Array(load);

        let actions = self
            .lock_action_metrics()
            .iter()
            .map(|(database_hash, metrics)| metrics.to_reporting_json(database_hash))
            .flatten()
            .collect();
        diagnostics["actions"] = JSONValue::Array(actions);

        let errors = self
            .lock_error_metrics()
            .iter()
            .map(|(database_hash, metrics)| metrics.to_reporting_json(database_hash))
            .flatten()
            .collect();
        diagnostics["errors"] = JSONValue::Array(errors);

        diagnostics
    }

    fn to_minimal_reporting_json(&self) -> JSONValue {
        let mut diagnostics = self.server_properties.to_reporting_json();
        diagnostics["server"] = self.server_metrics.to_reporting_minimal_json();
        diagnostics
    }

    pub fn to_monitoring_json(&self) -> JSONValue {
        let mut diagnostics = self.server_properties.to_monitoring_json();

        diagnostics["server"] = self.server_metrics.to_monitoring_json();

        let load = self
            .lock_load_metrics()
            .iter()
            .filter_map(|(database_hash, metrics)| {
                metrics.to_monitoring_json(database_hash, self.is_owned(database_hash))
            })
            .collect();
        diagnostics["load"] = JSONValue::Array(load);

        let actions = self
            .lock_action_metrics()
            .iter()
            .map(|(database_hash, metrics)| metrics.to_monitoring_json(database_hash))
            .flatten()
            .collect();
        diagnostics["actions"] = JSONValue::Array(actions);

        let errors = self
            .lock_error_metrics()
            .iter()
            .map(|(database_hash, metrics)| metrics.to_monitoring_json(database_hash))
            .flatten()
            .collect();
        diagnostics["errors"] = JSONValue::Array(errors);

        diagnostics
    }

    pub fn to_prometheus_data(&self) -> String {
        let mut database_load_data = String::from(LoadMetrics::prometheus_header().to_owned() + "\n");
        let database_load_data_header_length = database_load_data.len();
        for (database_hash, metrics) in self.lock_load_metrics().iter() {
            database_load_data.push_str(&metrics.to_prometheus_data(database_hash, self.is_owned(database_hash)));
        }

        let mut requests_data_attempted = String::from(ActionMetrics::prometheus_header_attempted().to_owned() + "\n");
        let requests_data_attempted_header_length = requests_data_attempted.len();
        for (database_hash, metrics) in self.lock_action_metrics().iter() {
            requests_data_attempted.push_str(&metrics.to_prometheus_data_attempted(database_hash));
        }

        let mut requests_data_successful =
            String::from(ActionMetrics::prometheus_header_successful().to_owned() + "\n");
        let requests_data_successful_header_length = requests_data_successful.len();
        for (database_hash, metrics) in self.lock_action_metrics().iter() {
            requests_data_successful.push_str(&metrics.to_prometheus_data_successful(database_hash));
        }

        let mut user_errors_data = String::from(ErrorMetrics::prometheus_header().to_owned() + "\n");
        let user_errors_data_header_length = user_errors_data.len();
        for (database_hash, metrics) in self.lock_error_metrics().iter() {
            user_errors_data.push_str(&metrics.to_prometheus_data(database_hash));
        }

        vec![
            self.server_properties.to_prometheus_comment() + &self.server_metrics.to_prometheus_comment(),
            ServerMetrics::prometheus_header().to_owned(),
            self.server_metrics.to_prometheus_data(),
            if database_load_data.len() > database_load_data_header_length {
                database_load_data
            } else {
                String::new()
            },
            if requests_data_attempted.len() > requests_data_attempted_header_length {
                requests_data_attempted
            } else {
                String::new()
            },
            if requests_data_successful.len() > requests_data_successful_header_length {
                requests_data_successful
            } else {
                String::new()
            },
            if user_errors_data.len() > user_errors_data_header_length { user_errors_data } else { String::new() },
        ]
        .join("\n")
    }

    fn hash_database(database_name: impl AsRef<str> + Hash) -> DatabaseHash {
        hash_string_consistently(database_name)
    }

    fn hash_database_opt(database_name: Option<impl AsRef<str> + Hash>) -> DatabaseHashOpt {
        match database_name {
            None => None,
            Some(database_name) => Some(Self::hash_database(database_name)),
        }
    }

    fn update_owned_databases(&self, database_hash: DatabaseHash, is_primary_server: bool) {
        let mut owned_databases = self.owned_databases.lock().expect("Expected owned databases lock acquisition");
        if is_primary_server {
            owned_databases.insert(database_hash);
        } else {
            owned_databases.remove(&database_hash);
        }
    }

    fn lock_load_metrics(&self) -> MutexGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.lock().expect("Expected load metrics lock acquisition")
    }

    fn lock_action_metrics(&self) -> MutexGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics.lock().expect("Expected action metrics lock acquisition")
    }

    fn lock_error_metrics(&self) -> MutexGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics.lock().expect("Expected error metrics lock acquisition")
    }

    fn is_owned(&self, database_hash: &DatabaseHash) -> bool {
        self.owned_databases.lock().expect("Expected owned databases lock acquisition").contains(database_hash)
    }
}

// Used when the hash has to be consistent over time and restarts (default hasher does not suit)
pub fn hash_string_consistently(value: impl AsRef<str> + Hash) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(value.as_ref().as_bytes());
    hasher.digest()
}
