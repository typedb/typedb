/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::rc::Rc;
use std::sync::Arc;

use database::Database;
use database::transaction::TransactionRead;
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
    let db = Arc::new(db_result.unwrap());

    let txn = TransactionRead::open(db.clone());
    let types = txn.type_manager();
    let root_entity_type = types.get_entity_type(&Kind::Entity.root_label());
    eprintln!("Root entity type: {:?}", root_entity_type);
    // let delete_result = db.delete();
    // assert!(delete_result.is_ok());
}
