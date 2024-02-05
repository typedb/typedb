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

use storage::key_value::{StorageKey, StorageKey, StorageValue};
use storage::*;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn snapshot_buffered_put_get() {
    init_logging();

    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_keyspace("sec", sec_id, &storage::StorageSection::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    let value_1 = Box::new([0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), StorageValue::Value(value_1.clone()));
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(snapshot.get(&key_1), Some(StorageValue::Value(value_1)));
    assert_eq!(snapshot.get(&key_2), Some(StorageValue::Empty));

    let key_5 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0xff, 0xff, 0xff], sec_id)));
    assert_eq!(snapshot.get(&key_5), None);

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_put_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_keyspace("sec", sec_id, &storage::StorageSection::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    let key_prefix = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, StorageValue)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), StorageValue::Empty),
            (Box::from(key_3.bytes()), StorageValue::Empty),
            (Box::from(key_4.bytes()), StorageValue::Empty),
        ]
    );

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_delete() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_keyspace("sec", sec_id, &storage::StorageSection::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    snapshot.delete(key_3.clone());

    assert_eq!(snapshot.get(&key_3), None);

    let key_prefix = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, StorageValue)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), StorageValue::Empty),
            (Box::from(key_4.bytes()), StorageValue::Empty),
        ]
    );

    delete_dir(storage_path)
}

#[test]
fn snapshot_read_through() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_keyspace("sec", sec_id, &storage::StorageSection::new_db_options()).unwrap();

    let key_1 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));

    let snapshot = storage.snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit();

    let key_5 = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1, 0x2, 0x0], sec_id)));

    // test put - iterate read-through
    let snapshot = storage.snapshot_write();
    snapshot.put(key_5.clone());

    let key_prefix = StorageKey::Fixed(StorageKey::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, StorageValue)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), StorageValue::Empty),
            (Box::from(key_3.bytes()), StorageValue::Empty),
            (Box::from(key_5.bytes()), StorageValue::Empty),
            (Box::from(key_4.bytes()), StorageValue::Empty),
        ]
    );

    // test delete-iterate read-through
    snapshot.delete(key_2.clone());
    let key_values: Vec<(Box<[u8]>, StorageValue)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_3.bytes()), StorageValue::Empty),
            (Box::from(key_5.bytes()), StorageValue::Empty),
            (Box::from(key_4.bytes()), StorageValue::Empty),
        ]
    );

    delete_dir(storage_path)
}
