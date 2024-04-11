/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod test_common;

use std::path::Path;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use durability::wal::WAL;
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{IsolationError, IsolationConflict},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::SnapshotError,
    KeyspaceSet, MVCCStorage,
};
use storage::keyspace::KeyspaceId;
use storage::snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot};
use test_utils::{create_tmp_dir, init_logging};


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
    snapshot.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_1)), ByteArray::copy(&VALUE_1));
    snapshot.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_2)), ByteArray::copy(&VALUE_2));
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
    snapshot_1.put_val(key_3.clone(), value_3.clone());
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
#[test]
fn g0_update_conflicts_fail() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);

    let snapshot_1 = storage.open_snapshot_write();
    let snapshot_2 = storage.open_snapshot_write();

    let key_1 = StorageKey::Reference(StorageKeyReference::new(Keyspace, ByteReference::new(&KEY_1)));

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
                            source: IsolationError::Conflict(IsolationConflict::DeletingRequiredKey),
                            ..
                        },
                        ..
                    },
                    ..
                },
                ..
            )
        ),
        "{:?}",
        result_2
    );
}

#[test]
fn isolation_manager_reads_evicted_from_disk() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);
    let key_1 = StorageKey::new_owned(Keyspace, ByteArray::copy(&KEY_1));
    let key_2 = StorageKey::new_owned(Keyspace, ByteArray::copy(&KEY_2));
    let value_1 = ByteArray::copy(&VALUE_1);

    let snapshot0 = storage.open_snapshot_write();
    snapshot0.put_val(key_1.clone().into_owned_array(), value_1.clone());
    snapshot0.commit().unwrap();
    let watermark_after_0 = storage.read_watermark();

    let snapshot1 = storage.open_snapshot_write();
    snapshot1.delete(key_1.clone().into_owned_array());
    snapshot1.commit().unwrap();

    for _i in 0..resource::constants::storage::TIMELINE_WINDOW_SIZE {
        let snapshot_i = storage.open_snapshot_write();
        snapshot_i.put_val(key_2.clone().into_owned_array(), value_1.clone());
        snapshot_i.commit().unwrap();
    }

    {
        let snapshot_passes = storage.open_snapshot_write_at(watermark_after_0).unwrap();
        snapshot_passes.put_val(key_2.clone().into_owned_array(), value_1.clone());
        let snapshot_passes_result = snapshot_passes.commit();

        assert!(snapshot_passes_result.is_ok());
    }
    {
        let snapshot_conflicts = storage.open_snapshot_write_at(watermark_after_0).unwrap();
        snapshot_conflicts.get_required(key_1.clone()).unwrap();
        snapshot_conflicts.put_val(key_2.clone().into_owned_array(), value_1.clone());
        let snapshot_conflicts_result = snapshot_conflicts.commit();

        assert!(
            matches!(
                snapshot_conflicts_result,
                Err(
                    SnapshotError::Commit {
                    source: MVCCStorageError {
                        kind: MVCCStorageErrorKind::IsolationError {
                            source: IsolationError::Conflict(IsolationConflict::RequireDeletedKey),
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
            snapshot_conflicts_result.unwrap_err()
        );
    }
}
