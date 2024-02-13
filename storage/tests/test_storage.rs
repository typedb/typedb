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

use rand;
use bytes::byte_array::ByteArray;

use storage::error::{MVCCStorageError, MVCCStorageErrorKind};
use storage::key_value::{StorageKeyArray, StorageKeyReference, StorageValue};
use storage::MVCCStorage;
use storage::keyspace::keyspace::{KeyspaceId};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn create_delete() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage_result = MVCCStorage::new(Rc::from("storage"), &storage_path);
    assert!(storage_result.is_ok());
    let storage = storage_result.unwrap();
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok());
    delete_dir(storage_path)
}

#[test]
fn create_keyspaces() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_1_id: KeyspaceId = 0x1;
    let create_1_result = storage.create_keyspace("keyspace_1", keyspace_1_id, &options);
    assert!(create_1_result.is_ok());
    let keyspace_2_id: KeyspaceId = 0x10;
    let create_2_result = storage.create_keyspace("keyspace_2", keyspace_2_id, &options);
    assert!(create_2_result.is_ok(), "{create_2_result:?}");
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok(), "{:?}", delete_result);

    delete_dir(storage_path)
}

#[test]
fn create_keyspaces_errors() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_1_id: KeyspaceId = 0x1;
    storage.create_keyspace("keyspace_1", keyspace_1_id, &options).unwrap();

    let keyspace_2_id: KeyspaceId = 0x10;
    let name_error = storage.create_keyspace("keyspace_1", keyspace_2_id, &options);
    assert!(matches!(name_error, Err(MVCCStorageError{
        kind: MVCCStorageErrorKind::KeyspaceNameExists {
            ..
        },
        ..
    })), "{}", name_error.unwrap_err());

    let duplicate_prefix: KeyspaceId = 0x1;
    let prefix_error = storage.create_keyspace("keyspace_2", duplicate_prefix, &options);
    dbg!(&prefix_error);
    assert!(matches!(prefix_error, Err(MVCCStorageError{
        kind: MVCCStorageErrorKind::KeyspaceIdExists {
            ..
        },
        ..
    })), "{}", prefix_error.unwrap_err());

    delete_dir(storage_path)
}

#[test]
fn get_put_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let keyspace_1_id: u8 = 0x1;
    storage.create_keyspace("keyspace_1", keyspace_1_id, &options).unwrap();
    let keyspace_2_id: u8 = 0x10;
    storage.create_keyspace("keyspace_2", keyspace_2_id, &options).unwrap();

    let keyspace_1_key_1 = StorageKeyArray::<64>::from((vec![0x0, 0x0, 0x1], keyspace_1_id));
    let keyspace_1_key_2 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x10], keyspace_1_id));
    let keyspace_1_key_3 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0xff], keyspace_1_id));
    let keyspace_1_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], keyspace_1_id));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_1), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_2), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_3), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_4), &StorageValue::<'_, 128>::empty());

    let keyspace_2_key_1 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x1], keyspace_2_id));
    let keyspace_2_key_2 = StorageKeyArray::<64>::from((vec![0xb, 0x0, 0x10], keyspace_2_id));
    let keyspace_2_key_3 = StorageKeyArray::<64>::from((vec![0x5, 0x0, 0xff], keyspace_2_id));
    let keyspace_2_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], keyspace_2_id));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_1), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_2), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_3), &StorageValue::<'_, 128>::empty());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_4), &StorageValue::<'_, 128>::empty());

    let first_value = storage.get_raw(StorageKeyReference::from(&keyspace_1_key_1), |value| StorageValue::from(Some(Box::from(value))));
    assert_eq!(first_value, Some(StorageValue::<'_, 128>::empty()));

    let second_value = storage.get_raw(StorageKeyReference::from(&keyspace_2_key_1), |value| StorageValue::<'_, 128>::from(Some(Box::from(value))));
    assert_eq!(second_value, Some(StorageValue::empty()));

    let prefix = StorageKeyArray::<64>::from((vec![0x1], keyspace_1_id));
    let items: Vec<(ByteArray<64>, ByteArray<128>)> = storage.iterate_keyspace_prefix(StorageKeyReference::from(&prefix)).collect_cloned::<64, 128>();
    assert_eq!(items, vec![
        (keyspace_1_key_2.into_byte_array(), ByteArray::<128>::empty()),
        (keyspace_1_key_3.into_byte_array(), ByteArray::<128>::empty()),
    ]);

    delete_dir(storage_path)
}

