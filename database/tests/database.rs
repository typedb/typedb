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

use std::rc::Rc;

use database::database::Database;
use encoding::graph::type_::Root;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn create_delete_database() {
    init_logging();
    let database_path = create_tmp_dir();
    let db_result = Database::new(&database_path, Rc::from("create_delete"));
    assert!(db_result.is_ok());
    let db = db_result.unwrap();

    let txn = db.transaction_read();
    let types = txn.type_manager();
    let root_entity_type = types.get_entity_type(Root::Entity.label());
    dbg!("Root entity type: {}", root_entity_type);
    // let delete_result = db.delete();
    // assert!(delete_result.is_ok());
    delete_dir(database_path)
}
