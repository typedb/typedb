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

use bytes::byte_array::ByteArray;
use logger::result::ResultExt;
use storage::*;
use storage::key_value::{StorageKey, StorageKeyArray, StorageValue, StorageValueArray};
use storage::keyspace::keyspace::KeyspaceId;
use storage::snapshot::buffer::{BUFFER_INLINE_KEY, BUFFER_INLINE_VALUE};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn snapshot_buffered_put_get() {
    init_logging();

    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    let value_1 = ByteArray::from(&[0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), StorageValueArray::new(value_1.clone()));
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(snapshot.get(&StorageKey::Array(key_1)), Some(StorageValueArray::new(value_1)));
    assert_eq!(snapshot.get(&StorageKey::Array(key_2)), Some(StorageValueArray::empty()));

    let key_5 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0xff, 0xff, 0xff], keyspace_id));
    assert_eq!(snapshot.get(&StorageKey::Array(key_5)), None);
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_put_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    let key_prefix = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1], keyspace_id));
    let items: Vec<(StorageKey<'_, BUFFER_INLINE_KEY>, StorageValue<'_, BUFFER_INLINE_VALUE>)> = snapshot.iterate_prefix(&StorageKey::Array(key_prefix)).collect_cloned();
    assert_eq!(
        items,
        vec![
            (StorageKey::Array(key_2), StorageValue::empty()),
            (StorageKey::Array(key_3), StorageValue::empty()),
        ]
    );
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_delete() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    snapshot.delete(key_3.clone());

    assert_eq!(snapshot.get(&StorageKey::Array(key_3)), None);

    let key_prefix = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1], keyspace_id));
    let items: Vec<(StorageKey<'_, BUFFER_INLINE_KEY>, StorageValue<'_, BUFFER_INLINE_VALUE>)> = snapshot.iterate_prefix(&StorageKey::Array(key_prefix)).collect_cloned();
    assert_eq!(
        items,
        vec![
            (StorageKey::Array(key_2), StorageValue::empty()),
        ]
    );
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_read_through() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x2, 0x0, 0xff], keyspace_id));

    let snapshot = storage.open_snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit().unwrap_or_log();

    let key_5 = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1, 0x2, 0x0], keyspace_id));

    // test put - iterate read-through
    let snapshot = storage.open_snapshot_write();
    snapshot.put(key_5.clone());

    let key_prefix = StorageKeyArray::<BUFFER_INLINE_KEY>::from((vec![0x1], keyspace_id));
    let key_values: Vec<(StorageKey<'_, BUFFER_INLINE_KEY>, StorageValue<'_, BUFFER_INLINE_VALUE>)> = snapshot.iterate_prefix(&StorageKey::Array(key_prefix.clone())).collect_cloned();
    assert_eq!(
        key_values,
        vec![
            (StorageKey::Array(key_2.clone()), StorageValue::empty()),
            (StorageKey::Array(key_3.clone()), StorageValue::empty()),
            (StorageKey::Array(key_5.clone()), StorageValue::empty()),
        ]
    );

    // test delete-iterate read-through
    snapshot.delete(key_2.clone());
    let key_values: Vec<(StorageKey<'_, BUFFER_INLINE_KEY>, StorageValue<'_, BUFFER_INLINE_VALUE>)> =  snapshot.iterate_prefix(&StorageKey::Array(key_prefix)).collect_cloned();
    assert_eq!(
        key_values,
        vec![
            (StorageKey::Array(key_3), StorageValue::empty()),
            (StorageKey::Array(key_5), StorageValue::empty()),
        ]
    );
    snapshot.close_resources();

    delete_dir(storage_path)
}
