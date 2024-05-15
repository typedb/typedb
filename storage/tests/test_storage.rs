/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use bytes::{byte_array::ByteArray, Bytes};
use durability::wal::WAL;
use itertools::Itertools;
use storage::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{KeyspaceOpenError, KeyspaceValidationError},
    StorageOpenError,
};
use test_utils::{create_tmp_dir, init_logging};

use crate::test_common::{checkpoint_storage, create_storage, load_storage};

mod test_common;

#[test]
fn create_delete() {
    test_keyspace_set! {}

    init_logging();
    let storage_path = create_tmp_dir();
    let storage_result = create_storage::<TestKeyspaceSet>(&storage_path);

    assert!(storage_result.is_ok());
    let storage = Arc::into_inner(storage_result.unwrap()).unwrap();
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
    let storage_result = create_storage::<TestKeyspaceSet>(&storage_path);

    assert!(storage_result.is_ok(), "{storage_result:?}");

    let storage = Arc::into_inner(storage_result.unwrap()).unwrap();

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
    let storage_result = create_storage::<TestKeyspaceSet>(&storage_path);
    assert!(
        matches!(
            storage_result,
            Err(StorageOpenError::KeyspaceOpen {
                source: KeyspaceOpenError::Validation { source: KeyspaceValidationError::NameExists { .. }, .. },
                ..
            })
        ),
        "{}",
        storage_result.unwrap_err()
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
    let storage_result = create_storage::<TestKeyspaceSet>(&storage_path);
    assert!(
        matches!(
            storage_result,
            Err(StorageOpenError::KeyspaceOpen {
                source: KeyspaceOpenError::Validation { source: KeyspaceValidationError::IdExists { .. }, .. },
                ..
            })
        ),
        "{}",
        storage_result.unwrap_err()
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
        .map(|bytes| StorageKeyArray::<64>::from((Keyspace, bytes)))
        .collect_vec();

    init_logging();
    let storage_path = create_tmp_dir();
    let mut checkpoint = None;
    {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();
        for key in &keys {
            storage.put_raw(StorageKeyReference::from(key), &empty_value());
        }
        checkpoint = Some(checkpoint_storage(&storage));
    }

    {
        let storage = load_storage::<TestKeyspaceSet>(
            &storage_path,
            WAL::load(&storage_path).unwrap(),
            Some(checkpoint.unwrap()),
        )
        .unwrap();
        let items = storage
            .iterate_keyspace_range(KeyRange::new_unbounded(StorageKey::<64>::Reference(StorageKeyReference::from(
                &StorageKeyArray::<64>::from((Keyspace, [0x0])),
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
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let keyspace_1_key_1 = StorageKeyArray::<64>::from((Keyspace1, [0x0, 0x0, 0x1]));
    let keyspace_1_key_2 = StorageKeyArray::<64>::from((Keyspace1, [0x1, 0x0, 0x10]));
    let keyspace_1_key_3 = StorageKeyArray::<64>::from((Keyspace1, [0x1, 0x0, 0xff]));
    let keyspace_1_key_4 = StorageKeyArray::<64>::from((Keyspace1, [0x2, 0x0, 0xff]));
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_1), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_2), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_3), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_1_key_4), &empty_value());

    let keyspace_2_key_1 = StorageKeyArray::<64>::from((Keyspace2, [0x1, 0x0, 0x1]));
    let keyspace_2_key_2 = StorageKeyArray::<64>::from((Keyspace2, [0xb, 0x0, 0x10]));
    let keyspace_2_key_3 = StorageKeyArray::<64>::from((Keyspace2, [0x5, 0x0, 0xff]));
    let keyspace_2_key_4 = StorageKeyArray::<64>::from((Keyspace2, [0x2, 0x0, 0xff]));
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_1), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_2), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_3), &empty_value());
    storage.put_raw(StorageKeyReference::from(&keyspace_2_key_4), &empty_value());

    let first_value: Option<ByteArray<48>> =
        storage.get_raw_mapped(StorageKeyReference::from(&keyspace_1_key_1), ByteArray::copy);
    assert_eq!(first_value, Some(ByteArray::empty()));

    let second_value: Option<ByteArray<48>> =
        storage.get_raw_mapped(StorageKeyReference::from(&keyspace_2_key_1), ByteArray::copy);
    assert_eq!(second_value, Some(ByteArray::empty()));

    let prefix = StorageKeyArray::<64>::from((Keyspace1, [0x1]));
    let items: Vec<(ByteArray<64>, ByteArray<128>)> = storage
        .iterate_keyspace_range(KeyRange::new_within(
            StorageKey::<64>::Reference(StorageKeyReference::from(&prefix)),
            false,
        ))
        .collect_cloned::<64, 128>();
    assert_eq!(
        items,
        vec![
            (keyspace_1_key_2.into_byte_array(), ByteArray::<128>::empty()),
            (keyspace_1_key_3.into_byte_array(), ByteArray::<128>::empty()),
        ]
    );
}
