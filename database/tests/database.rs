/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{Database, database_manager::DatabaseManager};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use options::ByteSize;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, create_tmp_storage_dir, init_logging};
use test_utils_storage::create_rocks_resources;

#[test]
fn create_delete_database() {
    init_logging();
    let database_path = create_tmp_storage_dir();
    let diagnostics_manager = Arc::new(DiagnosticsManager::new_disabled());
    let resources = create_rocks_resources();
    let db_result =
        Database::<WALClient>::open(&database_path.join("create_delete"), &diagnostics_manager, &resources);
    assert!(db_result.is_ok(), "{:?}", db_result.unwrap_err());
    let db = db_result.unwrap();
    let delete_result = db.delete();
    assert!(delete_result.is_ok());
}

#[test]
fn prepare_for_writes_iterates_every_loaded_database() {
    init_logging();
    let data_dir = create_tmp_dir("prepare_for_writes");
    let dbm = DatabaseManager::new(
        &data_dir,
        Arc::new(DiagnosticsManager::new_disabled()),
        ByteSize::mb(64),
        ByteSize::mb(64),
    )
    .expect("DatabaseManager::new");
    for name in ["alpha", "beta", "gamma"] {
        dbm.put_database_unrestricted(name).expect("put_database");
    }

    dbm.prepare_for_writes().expect("prepare_for_writes on freshly created databases must succeed");

    for name in ["alpha", "beta", "gamma"] {
        let db = dbm.database_unrestricted(name).expect("database still resolvable");
        assert_eq!(db.name(), name);
    }
}
