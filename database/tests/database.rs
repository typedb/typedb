/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::path::PathBuf;
use std::rc::Rc;

use encoding::type_::type_encoding::concept::root::Root;
use logger::initialise_logging;
use rand;
use tracing::dispatcher::DefaultGuard;

use database::database::Database;

fn setup() -> (PathBuf, DefaultGuard) {
    let guard = initialise_logging();
    let id = rand::random::<u64>();
    let fs_tmp_dir = std::env::temp_dir();
    (fs_tmp_dir.with_extension(format!("test_database{}", id)), guard)
}

fn cleanup(path: PathBuf) {
    std::fs::remove_dir_all(path).ok();
}

#[test]
fn create_delete_database() {
    let (database_path, _log_guard) = setup();
    let db_result = Database::new(&database_path, Rc::from("create_delete"));
    assert!(db_result.is_ok());
    let db = db_result.unwrap();

    let txn = db.transaction_read();
    let types = txn.type_manager();
    let root_entity_type = types.get_entity_type(&Root::Entity.label());
    dbg!("Root entity type: {}", root_entity_type);
    // let delete_result = db.delete();
    // assert!(delete_result.is_ok());
    cleanup(database_path)
}
