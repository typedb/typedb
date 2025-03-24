/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    path::PathBuf,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use serde_json::Value as JSONValue;
use xxhash_rust::xxh3::Xxh3;

use crate::{
    metrics::{
        client_endpoints_map, ActionKind, ActionMetrics, ClientEndpoint, DatabaseMetrics, ErrorMetrics, LoadKind,
        LoadMetrics, ServerMetrics, ServerProperties, ALL_CLIENT_ENDPOINTS,
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

type DatabaseHash = u64;
type DatabaseHashOpt = Option<u64>;

#[derive(Debug)]
pub struct Diagnostics {
    server_properties: ServerProperties,
    server_metrics: ServerMetrics,
    load_metrics: RwLock<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ActionMetrics>>>,
    error_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ErrorMetrics>>>,

    is_full_reporting: bool,
    owned_databases: RwLock<HashSet<DatabaseHash>>,
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
            load_metrics: RwLock::new(HashMap::new()),
            action_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            error_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),

            is_full_reporting: is_reporting_enabled,
            owned_databases: RwLock::new(HashSet::new()),
        }
    }

    pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>) {
        let mut loads = self.lock_load_metrics_write();
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

    pub fn increment_load_count(
        &self,
        client: ClientEndpoint,
        database_name: impl AsRef<str> + Hash,
        load_kind: LoadKind,
    ) {
        let database_hash = Self::hash_database(database_name);
        let loads = self.lock_load_metrics_read_for_database(database_hash);
        loads.get(&database_hash).expect("Expected database in loads").increment_connection_count(client, load_kind);
    }

    pub fn decrement_load_count(
        &self,
        client: ClientEndpoint,
        database_name: impl AsRef<str> + Hash,
        load_kind: LoadKind,
    ) {
        let database_hash = Self::hash_database(database_name);
        let loads = self.lock_load_metrics_read_for_database(database_hash);
        loads.get(&database_hash).expect("Expected database in loads").decrement_connection_count(client, load_kind);
    }

    pub fn submit_action_success(
        &self,
        client: ClientEndpoint,
        database_name: Option<impl AsRef<str> + Hash>,
        action_kind: ActionKind,
    ) {
        let database_hash = Self::hash_database_opt(database_name);
        let actions = self.lock_action_metrics_read_for_database(client, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_success(action_kind);
    }

    pub fn submit_action_fail(
        &self,
        client: ClientEndpoint,
        database_name: Option<impl AsRef<str> + Hash>,
        action_kind: ActionKind,
    ) {
        let database_hash = Self::hash_database_opt(database_name);
        let actions = self.lock_action_metrics_read_for_database(client, database_hash);
        actions.get(&database_hash).expect("Expected database in actions").submit_fail(action_kind);
    }

    pub fn submit_error(
        &self,
        client: ClientEndpoint,
        database_name: Option<impl AsRef<str> + Hash>,
        error_code: String,
    ) {
        let database_hash = Self::hash_database_opt(database_name);
        let errors = self.lock_error_metrics_read_for_database(client, database_hash);
        errors.get(&database_hash).expect("Expected database in errors").submit(error_code);
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

    fn hash_database(database_name: impl AsRef<str> + Hash) -> DatabaseHash {
        hash_string_consistently(database_name)
    }

    fn hash_database_opt(database_name: Option<impl AsRef<str> + Hash>) -> DatabaseHashOpt {
        database_name.map(Self::hash_database)
    }

    fn update_owned_databases(&self, database_hash: DatabaseHash, is_primary_server: bool) {
        let mut owned_databases = self.owned_databases.write().expect("Expected owned databases lock acquisition");
        if is_primary_server {
            owned_databases.insert(database_hash);
        } else {
            owned_databases.remove(&database_hash);
        }
    }

    fn is_owned(&self, database_hash: &DatabaseHash) -> bool {
        self.owned_databases.read().expect("Expected owned databases lock acquisition").contains(database_hash)
    }
}

macro_rules! generate_metric_functions {
    (
        $metrics_field:ident,
        $metrics_type:ty,
        $hash_type:ty,
        $metric_new_fn:expr,
        $lock_read_fn:ident,
        $lock_write_fn:ident,
        $lock_read_for_database_fn:ident,
        $try_lock_read_for_database_fn:ident,
        $add_database_fn:ident
    ) => {
        impl Diagnostics {
            fn $lock_read_for_database_fn(
                &self,
                database_hash: $hash_type,
            ) -> RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>> {
                if let Some(lock) = self.$try_lock_read_for_database_fn(database_hash) {
                    return lock;
                }
                self.$add_database_fn(database_hash);
                self.$try_lock_read_for_database_fn(database_hash)
                    .expect("Expected metrics lock acquisition for database after adding")
            }

            fn $try_lock_read_for_database_fn(
                &self,
                database_hash: $hash_type,
            ) -> Option<RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>>> {
                let read_lock = self.$lock_read_fn();
                match read_lock.contains_key(&database_hash) {
                    true => Some(read_lock),
                    false => None,
                }
            }

            fn $add_database_fn(&self, database_hash: $hash_type) {
                let mut write_lock = self.$lock_write_fn();
                if !write_lock.contains_key(&database_hash) {
                    write_lock.insert(database_hash, $metric_new_fn());
                }
            }

            fn $lock_read_fn(&self) -> RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>> {
                self.$metrics_field.read().expect("Expected read lock acquisition")
            }

            fn $lock_write_fn(&self) -> RwLockWriteGuard<'_, HashMap<$hash_type, $metrics_type>> {
                self.$metrics_field.write().expect("Expected write lock acquisition")
            }
        }
    };
    (
        $metrics_field:ident,
        $metrics_type:ty,
        $hash_type:ty,
        $metric_new_fn:expr,
        $lock_read_fn:ident,
        $lock_write_fn:ident,
        $lock_read_for_database_fn:ident,
        $try_lock_read_for_database_fn:ident,
        $add_database_fn:ident,
        $client_type:ty
    ) => {
        impl Diagnostics {
            fn $lock_read_for_database_fn(
                &self,
                client: $client_type,
                database_hash: $hash_type,
            ) -> RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>> {
                if let Some(lock) = self.$try_lock_read_for_database_fn(client, database_hash) {
                    return lock;
                }
                self.$add_database_fn(client, database_hash);
                self.$try_lock_read_for_database_fn(client, database_hash)
                    .expect("Expected metrics lock acquisition for database after adding")
            }

            fn $try_lock_read_for_database_fn(
                &self,
                client: $client_type,
                database_hash: $hash_type,
            ) -> Option<RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>>> {
                let read_lock = self.$lock_read_fn(client);
                match read_lock.contains_key(&database_hash) {
                    true => Some(read_lock),
                    false => None,
                }
            }

            fn $add_database_fn(&self, client: $client_type, database_hash: $hash_type) {
                let mut write_lock = self.$lock_write_fn(client);
                if !write_lock.contains_key(&database_hash) {
                    write_lock.insert(database_hash, $metric_new_fn());
                }
            }

            fn $lock_read_fn(&self, client: $client_type) -> RwLockReadGuard<'_, HashMap<$hash_type, $metrics_type>> {
                self.$metrics_field
                    .get(&client)
                    .expect("Expected client {client}")
                    .read()
                    .expect("Expected read lock acquisition")
            }

            fn $lock_write_fn(&self, client: $client_type) -> RwLockWriteGuard<'_, HashMap<$hash_type, $metrics_type>> {
                self.$metrics_field
                    .get(&client)
                    .expect("Expected client {client}")
                    .write()
                    .expect("Expected write lock acquisition")
            }
        }
    };
}

generate_metric_functions!(
    load_metrics,
    LoadMetrics,
    DatabaseHash,
    LoadMetrics::new,
    lock_load_metrics_read,
    lock_load_metrics_write,
    lock_load_metrics_read_for_database,
    try_lock_load_metrics_read_for_database,
    add_database_to_load_metrics
);

generate_metric_functions!(
    action_metrics,
    ActionMetrics,
    DatabaseHashOpt,
    ActionMetrics::new,
    lock_action_metrics_read,
    lock_action_metrics_write,
    lock_action_metrics_read_for_database,
    try_lock_action_metrics_read_for_database,
    add_database_to_action_metrics,
    ClientEndpoint
);

generate_metric_functions!(
    error_metrics,
    ErrorMetrics,
    DatabaseHashOpt,
    ErrorMetrics::new,
    lock_error_metrics_read,
    lock_error_metrics_write,
    lock_error_metrics_read_for_database,
    try_lock_error_metrics_read_for_database,
    add_database_to_error_metrics,
    ClientEndpoint
);

// Used when the hash has to be consistent over time and restarts (default hasher does not suit)
pub fn hash_string_consistently(value: impl AsRef<str> + Hash) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(value.as_ref().as_bytes());
    hasher.digest()
}
