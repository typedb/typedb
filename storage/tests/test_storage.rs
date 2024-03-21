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

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef};
use durability::wal::WAL;
use primitive::prefix_range::PrefixRange;
use storage::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    KeyspaceSet, MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};

macro_rules! test_keyspace_set {
    {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
        enum TestKeyspaceSet { $($variant),* }
        impl KeyspaceSet for TestKeyspaceSet {
            fn iter() -> impl Iterator<Item = Self> { [$(Self::$variant),*].into_iter() }
            fn id(&self) -> u8 {
                match *self { $(Self::$variant => $id),* }
            }
            fn name(&self) -> &'static str {
                match *self { $(Self::$variant => $name),* }
            }
        }
    };
}

#[test]
fn create_delete() {
    test_keyspace_set! {}

    init_logging();
    let storage_path = create_tmp_dir();
    let storage_result = MVCCStorage::<WAL>::new::<TestKeyspaceSet>("storage", &storage_path);
    assert!(storage_result.is_ok());
    let storage = storage_result.unwrap();
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok());
}

#[test]
fn create_keyspaces() {
    test_keyspace_set! {
        Keyspace1 => 0: "keyspace_1",
        Keyspace2 => 1: "keyspace_2",
    }

    init_logging();
    let storage_path = create_tmp_dir();

    let create_result = MVCCStorage::<WAL>::new::<TestKeyspaceSet>("storage", &storage_path);
    assert!(create_result.is_ok(), "{create_result:?}");

    let storage = create_result.unwrap();

    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok(), "{delete_result:?}");
}

#[test]
fn create_keyspaces_duplicate_name_error() {
    test_keyspace_set! {
        Keyspace1 => 0: "keyspace_1",
        Keyspace2 => 1: "keyspace_1",
    }

    init_logging();
    let storage_path = create_tmp_dir();
    let create_result = MVCCStorage::<WAL>::new::<TestKeyspaceSet>("storage", &storage_path);
    assert!(
        matches!(create_result, Err(MVCCStorageError { kind: MVCCStorageErrorKind::KeyspaceNameExists { .. }, .. })),
        "{}",
        create_result.unwrap_err()
    );
}

#[test]
fn create_keyspaces_duplicate_id_error() {
    test_keyspace_set! {
        Keyspace1 => 0: "keyspace_1",
        Keyspace2 => 0: "keyspace_2",
    }

    init_logging();
    let storage_path = create_tmp_dir();
    let create_result = MVCCStorage::<WAL>::new::<TestKeyspaceSet>("storage", &storage_path);
    assert!(
        matches!(create_result, Err(MVCCStorageError { kind: MVCCStorageErrorKind::KeyspaceIdExists { .. }, .. })),
        "{}",
        create_result.unwrap_err()
    );
}

#[test]
fn get_put_iterate() {
    test_keyspace_set! {
        Keyspace1 => 0: "keyspace_1",
        Keyspace2 => 1: "keyspace_2",
    }
    use TestKeyspaceSet::{Keyspace1, Keyspace2};

    init_logging();
    let storage_path = create_tmp_dir();
    let storage = MVCCStorage::<WAL>::new::<TestKeyspaceSet>("storage", &storage_path).unwrap();

    let keyspace_1_key_1 = StorageKeyArray::<64>::from((vec![0x0, 0x0, 0x1], Keyspace1));
    let keyspace_1_key_2 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x10], Keyspace1));
    let keyspace_1_key_3 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0xff], Keyspace1));
    let keyspace_1_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], Keyspace1));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_1), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_2), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_3), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_4), &ByteArrayOrRef::Array(ByteArray::empty()));

    let keyspace_2_key_1 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x1], Keyspace2));
    let keyspace_2_key_2 = StorageKeyArray::<64>::from((vec![0xb, 0x0, 0x10], Keyspace2));
    let keyspace_2_key_3 = StorageKeyArray::<64>::from((vec![0x5, 0x0, 0xff], Keyspace2));
    let keyspace_2_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], Keyspace2));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_1), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_2), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_3), &ByteArrayOrRef::Array(ByteArray::empty()));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_4), &ByteArrayOrRef::Array(ByteArray::empty()));

    let first_value: Option<ByteArray<48>> =
        storage.get_raw(StorageKeyReference::from(&keyspace_1_key_1), ByteArray::copy);
    assert_eq!(first_value, Some(ByteArray::empty()));

    let second_value: Option<ByteArray<48>> =
        storage.get_raw(StorageKeyReference::from(&keyspace_2_key_1), ByteArray::copy);
    assert_eq!(second_value, Some(ByteArray::empty()));

    let prefix = StorageKeyArray::<64>::from((vec![0x1], Keyspace1));
    let items: Vec<(ByteArray<64>, ByteArray<128>)> = storage
        .iterate_keyspace_range(PrefixRange::new_within(StorageKey::<64>::Reference(StorageKeyReference::from(
            &prefix,
        ))))
        .collect_cloned::<64, 128>();
    assert_eq!(
        items,
        vec![
            (keyspace_1_key_2.into_byte_array(), ByteArray::<128>::empty()),
            (keyspace_1_key_3.into_byte_array(), ByteArray::<128>::empty()),
        ]
    );
}
