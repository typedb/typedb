/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use ::database::{
    Database,
    database::DatabaseCreateError,
    database_manager::{DatabaseManager, ImportOwnership},
    query::execute_schema_query,
    transaction::{CommitIntent, TransactionSchema},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use options::{MvccCleanupStrategy, TransactionOptions, byte_size::ByteSize};
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_storage_dir};

use crate::transaction::UnifiedTransactionView;

#[derive(Debug)]
pub struct Config {
    pub rocksdb_cache_size: ByteSize,
    pub rocksdb_write_buffers_limit: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self { rocksdb_cache_size: ByteSize::gb(1), rocksdb_write_buffers_limit: ByteSize::mb(512) }
    }
}

pub struct TypeDB {
    config: Config,
    _tmp_dir: TempDir,
    database_manager: Arc<DatabaseManager>,
}

impl TypeDB {
    pub fn init(config: Config) -> Self {
        let tmp_dir = create_tmp_storage_dir();
        let database_manager = DatabaseManager::new(
            &tmp_dir,
            Arc::new(DiagnosticsManager::new_disabled()),
            config.rocksdb_cache_size,
            config.rocksdb_write_buffers_limit,
            ImportOwnership::Exclusive,
            MvccCleanupStrategy::Disabled,
        )
        .unwrap();
        Self { config, _tmp_dir: tmp_dir, database_manager }
    }

    pub fn recreate_database(&self, name: &str) -> Result<Arc<Database<WALClient>>, Box<DatabaseCreateError>> {
        let new_name = name.replace("[", "").replace("]", "");
        let name = new_name.as_str();
        if self.database_manager.database(name).is_some() {
            self.database_manager.delete_database(name).unwrap();
        }
        self.database_manager.put_database(name)?;
        Ok(self.database_manager.database(name).unwrap())
    }
}

pub fn create_schema(database: Arc<Database<WALClient>>, schema: &str) {
    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}
