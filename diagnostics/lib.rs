/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::Mutex,
};

use error::TypeDBError;

use crate::metrics::{
    ActionKind, ActionMetrics, DatabaseMetrics, ErrorMetrics, LoadKind, LoadMetrics, ServerMetrics, ServerProperties,
};

pub mod diagnostics_manager;
pub mod metrics;
mod monitoring_server;
mod reporter;
mod version;

type DatabaseHash = Option<u64>;

#[derive(Debug)]
pub struct Diagnostics {
    server_properties: ServerProperties,
    server_metrics: ServerMetrics,
    load_metrics: Mutex<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: Mutex<HashMap<DatabaseHash, ActionMetrics>>,
    error_metrics: Mutex<HashMap<DatabaseHash, ErrorMetrics>>,

    owned_databases: Mutex<HashSet<DatabaseHash>>,
}

impl Diagnostics {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        reporting_enabled: bool,
    ) -> Diagnostics {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: Mutex::new(HashMap::new()),
            action_metrics: Mutex::new(HashMap::new()),
            error_metrics: Mutex::new(HashMap::new()),

            owned_databases: Mutex::new(HashSet::new()),
        }
    }

    pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>) {
        let mut loads = self.load_metrics.lock().expect("Expected load metrics lock acquisition");
        let mut deleted_databases: HashSet<DatabaseHash> = loads.keys().collect();

        for metrics in database_metrics {
            let database_hash = Self::hash_database(Some(metrics.database_name));
            deleted_databases.remove(&database_hash);

            let database_load = loads.entry(database_hash.clone()).or_insert(LoadMetrics::new());
            database_load.set_schema(metrics.schema);
            database_load.set_data(metrics.data);

            self.update_owned_databases(database_hash, metrics.is_primary_server);
        }

        for database_hash in deleted_databases {
            loads.get_mut(&database_hash).expect("Expected database in load metrics").mark_deleted();
        }
    }

    pub fn submit_error(&self, database_name: Option<&str>, error: &impl TypeDBError) {
        let database_hash = Self::hash_database(Some(database_name));
        let mut errors = self.error_metrics.lock().expect("Expected error metrics lock acquisition");
        errors.entry(database_hash).or_insert(ErrorMetrics::new()).submit(error);
    }

    pub fn submit_action_success(&self, database_name: Option<&str>, action_kind: ActionKind) {
        let database_hash = Self::hash_database(Some(database_name));
        let mut actions = self.action_metrics.lock().expect("Expected action metrics lock acquisition");
        actions.entry(database_hash).or_insert(ActionMetrics::new()).submit_success(action_kind);
    }

    pub fn submit_action_fail(&self, database_name: Option<&str>, action_kind: ActionKind) {
        let database_hash = Self::hash_database(Some(database_name));
        let mut actions = self.action_metrics.lock().expect("Expected action metrics lock acquisition");
        actions.entry(database_hash).or_insert(ActionMetrics::new()).submit_fail(action_kind);
    }

    pub fn increment_load_count(&self, database_name: Option<&str>, load_kind: LoadKind) {
        let database_hash = Self::hash_database(Some(database_name));
        let mut loads = self.load_metrics.lock().expect("Expected load metrics lock acquisition");
        loads.entry(database_hash).or_insert(LoadMetrics::new()).increment_connection_count(load_kind);
    }

    pub fn decrement_load_count(&self, database_name: Option<&str>, load_kind: LoadKind) {
        let database_hash = Self::hash_database(Some(database_name));
        let mut loads = self.load_metrics.lock().expect("Expected load metrics lock acquisition");
        loads.entry(database_hash).or_insert(LoadMetrics::new()).decrement_connection_count(load_kind);
    }

    fn hash_database(database_name: Option<impl AsRef<str>>) -> DatabaseHash {
        match database_name {
            None => None,
            Some(database_name) => {
                let mut database_name_hasher = DefaultHasher::new();
                database_name.hash(&mut database_name_hasher);
                Some(database_name_hasher.finish())
            }
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
}
