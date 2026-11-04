/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::Path, sync::Arc};

use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use options::{MvccCleanupStrategy, byte_size::ByteSize};
use storage::durability_client::WALClient;

pub fn manager_with_import_ownership(storage_dir: &Path, import_ownership: ImportOwnership) -> Arc<DatabaseManager> {
    DatabaseManager::new(
        storage_dir,
        Arc::new(DiagnosticsManager::new_disabled()),
        ByteSize::mb(64),
        ByteSize::mb(64),
        import_ownership,
        MvccCleanupStrategy::Disabled,
    )
    .unwrap()
}

pub fn manager(storage_dir: &Path) -> Arc<DatabaseManager> {
    manager_with_import_ownership(storage_dir, ImportOwnership::Exclusive)
}

pub fn create_db(storage_dir: &Path, db_name: &str) -> (Arc<DatabaseManager>, Arc<Database<WALClient>>) {
    let dbm = manager(storage_dir);
    dbm.put_database(db_name).unwrap();
    let database = dbm.database(db_name).unwrap();
    (dbm, database)
}

pub fn open_db(storage_dir: &Path, db_name: &str) -> (Arc<DatabaseManager>, Arc<Database<WALClient>>) {
    let dbm = manager(storage_dir);
    let database = dbm.database(db_name).unwrap();
    (dbm, database)
}
