/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use serde_json::Value as JSONValue;

use crate::{
    DatabaseHash, DatabaseHashOpt, DatabaseId, MonitoringSection, hash_string_consistently,
    metrics::{
        ALL_CLIENT_ENDPOINTS, ActionMetrics, ClientEndpoint, DatabaseHistograms, DatabaseHistogramsSnapshot,
        ErrorMetrics, LoadMetrics, ServerMetrics, ServerProperties, client_endpoints_map,
    },
    reports::{json_monitoring::to_monitoring_json, prometheus_monitoring::to_monitoring_prometheus},
};

#[derive(Debug)]
pub struct CoreMetrics {
    pub(crate) server_properties: ServerProperties,
    pub(crate) server_metrics: ServerMetrics,
    load_metrics: RwLock<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ActionMetrics>>>,
    error_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ErrorMetrics>>>,
    histogram_metrics: RwLock<HashMap<DatabaseHash, DatabaseHistograms>>,
}

impl CoreMetrics {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        is_reporting_enabled: bool,
    ) -> Self {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, is_reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: RwLock::new(HashMap::new()),
            action_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            error_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            histogram_metrics: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn lock_load_metrics_read_for_database(
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

    pub(crate) fn lock_load_metrics_read(&self) -> RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.read().expect("Expected read lock acquisition")
    }

    pub(crate) fn lock_load_metrics_write(&self) -> RwLockWriteGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.write().expect("Expected write lock acquisition")
    }

    pub(crate) fn lock_histogram_metrics_read_for_database(
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

    pub(crate) fn lock_action_metrics_read_for_database(
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

    pub(crate) fn lock_action_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }

    pub(crate) fn lock_error_metrics_read_for_database(
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

    pub(crate) fn lock_error_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }

    pub(crate) fn histogram_snapshots(&self) -> Vec<(Arc<DatabaseId>, DatabaseHistogramsSnapshot)> {
        self.lock_histogram_metrics_read().values().map(|db| (db.database_id().clone(), db.snapshot())).collect()
    }
}

impl MonitoringSection for CoreMetrics {
    fn name(&self) -> &str {
        "typedb"
    }

    fn write_prometheus(&self, out: &mut String) {
        out.push_str(&to_monitoring_prometheus(self));
    }

    fn write_json(&self) -> serde_json::Map<String, JSONValue> {
        to_monitoring_json(self)
    }
}
