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

use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;
use storage::error::{MVCCStorageError, MVCCStorageErrorKind};
use storage::isolation_manager::{IsolationError, IsolationErrorKind};
use storage::key_value::{StorageKey, StorageKeyArray, StorageKeyReference, StorageValueArray};
use storage::keyspace::keyspace::KeyspaceId;
use storage::MVCCStorage;
use storage::snapshot::error::{SnapshotError, SnapshotErrorKind};
use test_utils::{create_tmp_dir, init_logging};

const KEYSPACE_ID: KeyspaceId = 0;
const KEY_1: [u8; 4] = [0x0, 0x0, 0x0, 0x1];
const KEY_2: [u8; 4] = [0x0, 0x0, 0x0, 0x2];
const VALUE_1: [u8; 1] = [0x0];
const VALUE_2: [u8; 1] = [0x1];
const VALUE_3: [u8; 1] = [0x88];
const VALUE_4: [u8; 1] = [0x99];

fn setup_storage(storage_path: &PathBuf) -> MVCCStorage {
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    storage.create_keyspace("keyspace", KEYSPACE_ID, &MVCCStorage::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();
    snapshot.put_val(StorageKeyArray::new(KEYSPACE_ID, ByteArray::from(&KEY_1)), StorageValueArray::new(ByteArray::from(&VALUE_1)));
    snapshot.put_val(StorageKeyArray::new(KEYSPACE_ID, ByteArray::from(&KEY_2)), StorageValueArray::new(ByteArray::from(&VALUE_2)));
    snapshot.commit().unwrap();

    storage
}

///
/// Note: In general, we emulate an "update" as a 'getForUpdate' followed by a write
///

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

    let snapshot_1 = storage.snapshot_write();
    let snapshot_2 = storage.snapshot_write();

    let key_1: &StorageKey<'_, 48> = &StorageKey::Reference(StorageKeyReference::new(KEYSPACE_ID, ByteReference::new(&KEY_1)));
    let key_2: &StorageKey<'_, 48> = &StorageKey::Reference(StorageKeyReference::new(KEYSPACE_ID, ByteReference::new(&KEY_2)));

    snapshot_1.get_required(key_1);

    snapshot_2.delete(StorageKeyArray::from(key_1));

    let result_1 = snapshot_1.commit();
    assert!(result_1.is_ok());

    let result_2 = snapshot_2.commit();
    assert!(matches!(
        result_2,
        Err(SnapshotError {
            kind: SnapshotErrorKind::FailedCommit {
                source: MVCCStorageError {
                    kind: MVCCStorageErrorKind::IsolationError {
                        source: IsolationError {
                            kind: IsolationErrorKind::RequiredDeleteViolation
                        },
                        ..
                    },
                    ..
                },
                ..
            },
            ..
        })
    ), "{}", result_2.unwrap_err());
}
