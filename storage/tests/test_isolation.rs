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

use std::path::Path;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use durability::wal::WAL;
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{IsolationError, IsolationErrorKind},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::SnapshotError,
    KeyspaceSet, MVCCStorage,
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

test_keyspace_set! {
    Keyspace => 0: "keyspace",
}
use self::TestKeyspaceSet::Keyspace;

const KEY_1: [u8; 4] = [0x0, 0x0, 0x0, 0x1];
const KEY_2: [u8; 4] = [0x0, 0x0, 0x0, 0x2];
const KEY_3: [u8; 4] = [0x0, 0x0, 0x0, 0x3];
const VALUE_1: [u8; 1] = [0x0];
const VALUE_2: [u8; 1] = [0x1];
const VALUE_3: [u8; 1] = [0x88];

fn setup_storage(storage_path: &Path) -> MVCCStorage<WAL> {
    let storage = MVCCStorage::recover::<TestKeyspaceSet>("storage", storage_path).unwrap();

    let snapshot = storage.open_snapshot_write();
    snapshot.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_1)), ByteArray::copy(&VALUE_1)).unwrap();
    snapshot.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_2)), ByteArray::copy(&VALUE_2)).unwrap();
    snapshot.commit().unwrap();

    storage
}

#[test]
fn commits_isolated() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);

    let snapshot_1 = storage.open_snapshot_write();
    let snapshot_2 = storage.open_snapshot_read();

    let key_3 = StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_3));
    let value_3 = ByteArray::copy(&VALUE_3);
    snapshot_1.put_val(key_3.clone(), value_3.clone()).unwrap();
    snapshot_1.commit().unwrap();

    let get: Option<ByteArray<BUFFER_KEY_INLINE>> = snapshot_2.get(StorageKeyReference::from(&key_3)).unwrap();
    assert!(get.is_none());
    let prefix: StorageKey<'_, BUFFER_KEY_INLINE> =
        StorageKey::Array(StorageKeyArray::new(Keyspace, ByteArray::copy(&[0x0_u8])));
    let range = PrefixRange::new_within(prefix);
    let retrieved_count = snapshot_2.iterate_range(range.clone()).count();
    assert_eq!(retrieved_count, 2);

    let snapshot_3 = storage.open_snapshot_read();
    let get: Option<ByteArray<BUFFER_KEY_INLINE>> = snapshot_3.get(StorageKeyReference::from(&key_3)).unwrap();
    assert!(matches!(get, Some(_value_3)));
    let retrieved_count = snapshot_3.iterate_range(range.clone()).count();
    assert_eq!(retrieved_count, 3);
}

///
/// 1. Open two snapshots on the same version
/// 2. S1 - get_required(K1)
/// 3. S2 - delete(K1)
///
///
#[test]
fn g0_update_conflicts_fail() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);

    let snapshot_1 = storage.open_snapshot_write();
    let snapshot_2 = storage.open_snapshot_write();

    let key_1: StorageKey<'_, BUFFER_KEY_INLINE> =
        StorageKey::Reference(StorageKeyReference::new(Keyspace, ByteReference::new(&KEY_1)));
    let _key_2: StorageKey<'_, BUFFER_KEY_INLINE> =
        StorageKey::Reference(StorageKeyReference::new(Keyspace, ByteReference::new(&KEY_2)));

    snapshot_1.get_required(key_1.clone()).unwrap();

    snapshot_2.delete(key_1.clone().into_owned_array());

    let result_1 = snapshot_1.commit();
    assert!(result_1.is_ok());

    let result_2 = snapshot_2.commit();
    assert!(
        matches!(
            result_2,
            Err(
                SnapshotError::Commit {
                    source: MVCCStorageError {
                        kind: MVCCStorageErrorKind::IsolationError {
                            source: IsolationError { kind: IsolationErrorKind::RequiredDeleteViolation },
                            ..
                        },
                        ..
                    },
                    ..
                },
                ..
            )
        ),
        "{}",
        result_2.unwrap_err()
    );
}
