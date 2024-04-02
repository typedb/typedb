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

use bytes::{byte_array::ByteArray, Bytes};
use durability::wal::WAL;
use itertools::Itertools;
use primitive::prefix_range::PrefixRange;
use storage::{
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    KeyspaceSet, KeyspaceValidationError, MVCCStorage, StorageRecoverError,
};
use test_utils::{create_tmp_dir, init_logging};

macro_rules! test_keyspace_set {
    {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
        #[derive(Clone, Copy)]
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
    let storage_result = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path);
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

    let create_result = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path);
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
    let create_result = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path);
    assert!(
        matches!(
            create_result,
            Err(StorageRecoverError::KeyspaceValidation { source: KeyspaceValidationError::NameExists { .. } })
        ),
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
    let create_result = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path);
    assert!(
        matches!(
            create_result,
            Err(StorageRecoverError::KeyspaceValidation { source: KeyspaceValidationError::IdExists { .. } })
        ),
        "{}",
        create_result.unwrap_err()
    );
}

fn empty_value<const SZ: usize>() -> Bytes<'static, SZ> {
    Bytes::Array(ByteArray::empty())
}

#[test]
fn create_reopen() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }
    use TestKeyspaceSet::Keyspace;

    let keys = [[0x0, 0x0, 0x1], [0x1, 0x0, 0x10], [0x1, 0x0, 0xff], [0x2, 0x0, 0xff]]
        .into_iter()
        .map(|v| StorageKeyArray::<64>::from((v.as_slice(), Keyspace)))
        .collect_vec();

    init_logging();
    let storage_path = create_tmp_dir();

    {
        let storage = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();
        for key in &keys {
            storage.put_raw(StorageKeyReference::from(key), &empty_value());
        }
    }

    {
        let storage = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();
        let items: Vec<(ByteArray<64>, ByteArray<128>)> = storage
            .iterate_keyspace_range(PrefixRange::new_unbounded(StorageKey::<64>::Reference(StorageKeyReference::from(
                &StorageKeyArray::<64>::from((vec![0x0], Keyspace)),
            ))))
            .collect_cloned::<64, 128>();
        let items = items.into_iter().map(|(key, _)| key).collect_vec();
        assert_eq!(items, keys.into_iter().map(StorageKeyArray::into_byte_array).collect_vec());
    }
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
    let storage = MVCCStorage::<WAL>::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();

    let keyspace_1_key_1 = StorageKeyArray::<64>::from((vec![0x0, 0x0, 0x1], Keyspace1));
    let keyspace_1_key_2 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x10], Keyspace1));
    let keyspace_1_key_3 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0xff], Keyspace1));
    let keyspace_1_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], Keyspace1));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_1), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_2), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_3), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_4), &empty_value());

    let keyspace_2_key_1 = StorageKeyArray::<64>::from((vec![0x1, 0x0, 0x1], Keyspace2));
    let keyspace_2_key_2 = StorageKeyArray::<64>::from((vec![0xb, 0x0, 0x10], Keyspace2));
    let keyspace_2_key_3 = StorageKeyArray::<64>::from((vec![0x5, 0x0, 0xff], Keyspace2));
    let keyspace_2_key_4 = StorageKeyArray::<64>::from((vec![0x2, 0x0, 0xff], Keyspace2));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_1), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_2), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_3), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_4), &empty_value());

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
