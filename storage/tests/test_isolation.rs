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

use storage::key_value::{Key, KeyFixed, Value};
use storage::Storage;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn snapshot_buffered_put_get() {
    init_logging();

    let storage_path = create_tmp_dir();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_section("sec", sec_id, &storage::Section::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    let value_1 = Box::new([0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), Value::Value(value_1.clone()));
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(snapshot.get(&key_1), Some(Value::Value(value_1)));
    assert_eq!(snapshot.get(&key_2), Some(Value::Empty));

    let key_5 = Key::Fixed(KeyFixed::from((vec![sec_id, 0xff, 0xff, 0xff], sec_id)));
    assert_eq!(snapshot.get(&key_5), None);

    delete_dir(storage_path)
}
