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

use storage::key_value::{KeyspaceKey, SectionKeyFixed, Value};
use storage::{MVCCStorage, KeyspaceId};
use test_utils::{create_tmp_dir, init_logging};

const SEC_ID: u8 = 0;
const KEY_1: [u8; 4] = [SEC_ID, 0x0, 0x0, 0x1];
const KEY_2: [u8; 4] = [SEC_ID, 0x0, 0x0, 0x2];
const VALUE_1: [u8; 1]= [0x0];
const VALUE_2: [u8; 1]= [0x1];
//
// fn setup_storage(storage_path: &PathBuf) -> Storage {
//     let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
//     storage.create_section("sec", SEC_ID, &storage::Section::new_db_options()).unwrap();
//
//     let snapshot = storage.snapshot_write();
//     snapshot.put_val(Key::Fixed(KeyFixed::from(&KEY_1, SEC_ID)), Value::Value(Box::new(VALUE_1)));
//     snapshot.put_val(Key::Fixed(KeyFixed::from(&KEY_2, SEC_ID)), Value::Value(Box::new(VALUE_2)));
//     snapshot.commit();
//
//     storage
// }
//
// #[test]
// fn g0_update_conflicts_fail() {
//     init_logging();
//     let storage_path = create_tmp_dir();
//     let storage = setup_storage(&storage_path);
//
//     let snapshot_1 = storage.snapshot_write();
//     let snapshot_2 = storage.snapshot_write();
//
//
//
//
//     snapshot_1;
// }
