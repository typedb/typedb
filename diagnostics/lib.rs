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
    load_metrics: HashMap<DatabaseHash, LoadMetrics>, // TODO: Mutexes over these metrics?
    action_metrics: HashMap<DatabaseHash, ActionMetrics>, // TODO: Mutexes over these metrics?
    error_metrics: HashMap<DatabaseHash, ErrorMetrics>, // TODO: Mutexes over these metrics?

    owned_databases: HashSet<DatabaseHash>,
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
            load_metrics: HashMap::new(),
            action_metrics: HashMap::new(),
            error_metrics: HashMap::new(),

            owned_databases: HashSet::new(),
        }
    }

    pub fn submit_database_metrics(&self, database_metrics: HashSet<DatabaseMetrics>) {
        let mut deleted_databases: HashSet<DatabaseHash> = self.load_metrics.keys().collect();

        for metrics in database_metrics {
            let database_hash = self.hash_and_add_database(Some(metrics.database_name));
            deleted_databases.remove(&database_hash);

            let load_metrics =
                self.load_metrics.get_mut(&database_hash).expect("Expected to add database to load metrics");
            load_metrics.set_schema(metrics.schema);
            load_metrics.set_data(metrics.data);

            self.update_owned_databases(database_hash, metrics.is_primary_server);
        }

        for database_hash in deleted_databases {
            self.load_metrics.get_mut(&database_hash).expect("Expected to ... TODO").mark_deleted();
        }
    }

    pub fn submit_error(&self, database_name: Option<&str>, error: &impl TypeDBError) {
        todo!()
        // error.code();
    }

    pub fn submit_action_success(&self, database_name: Option<&str>, action_kind: ActionKind) {
        todo!()
    }

    pub fn submit_action_fail(&self, database_name: Option<&str>, action_kind: ActionKind) {
        todo!()
    }

    pub fn increment_current_count(&self, database_name: Option<&str>, connection_: LoadKind) {
        todo!()
    }

    pub fn decrement_current_count(&self, database_name: Option<&str>, connection_: LoadKind) {
        todo!()
    }

    fn hash_and_add_database(&self, database_name: Option<String>) -> DatabaseHash {
        let database_hash = match database_name {
            None => None,
            Some(database_name) => {
                let mut database_name_hasher = DefaultHasher::new();
                database_name.hash(&mut database_name_hasher);
                let database_hash = database_name_hasher.finish();
                // self.databaseLoad.computeIfAbsent(databaseHash, val -> new DatabaseLoadDiagnostics());
                Some(database_hash)
            }
        };

        // self.requests.computeIfAbsent(databaseHash, val -> new NetworkRequests());
        // self.userErrors.computeIfAbsent(databaseHash, val -> new UserErrorStatistics());

        database_hash
    }

    fn update_owned_databases(&self, database_hash: DatabaseHash, is_primary_server: bool) {
        match is_primary_server {
            false => self.owned_databases.remove(&database_hash),
            true => self.owned_databases.insert(database_hash),
        }
    }
}
