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

use database::Database;
use durability::wal::WAL;
use encoding::graph::type_::Kind;
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn create_delete_database() {
    init_logging();
    let database_path = create_tmp_dir();
    dbg!(database_path.exists());
    let db_result = Database::<WAL>::recover(&database_path, Rc::from("create_delete"));
    assert!(db_result.is_ok(), "{:?}", db_result.unwrap_err());
    let db = db_result.unwrap();

    let txn = db.transaction_read();
    let types = txn.type_manager();
    let root_entity_type = types.get_entity_type(&Kind::Entity.root_label());
    eprintln!("Root entity type: {:?}", root_entity_type);
    // let delete_result = db.delete();
    // assert!(delete_result.is_ok());
}
