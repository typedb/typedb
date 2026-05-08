/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::Database;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_storage::create_rocks_resources;

#[test]
fn create_delete_database() {
    init_logging();
    let database_path = create_tmp_storage_dir();
    let resources = create_rocks_resources();
    let db_result = Database::<WALClient>::open(&database_path.join("create_delete"), &resources);
    assert!(db_result.is_ok(), "{:?}", db_result.unwrap_err());
    let db = db_result.unwrap();
    let delete_result = db.delete();
    assert!(delete_result.is_ok());
}
