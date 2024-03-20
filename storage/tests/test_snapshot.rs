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

use bytes::byte_array::ByteArray;
use durability::wal::WAL;
use logger::result::ResultExt;
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_value::{StorageKey, StorageKeyArray},
    keyspace::keyspace::KeyspaceId,
    snapshot::error::SnapshotError,
    MVCCStorage,
};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn snapshot_buffered_put_get() {
    init_logging();

    let storage_path = create_tmp_dir();
    let options = MVCCStorage::<WAL>::new_db_options();
    let mut storage = MVCCStorage::<WAL>::new("storage", &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    let value_1 = ByteArray::copy(&[0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), value_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(snapshot.get(StorageKey::Array(key_1).as_reference()), Some(value_1));
    assert_eq!(snapshot.get::<48>(StorageKey::Array(key_2).as_reference()), Some(ByteArray::empty()));

    let key_5 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0xff, 0xff, 0xff], keyspace_id));
    assert_eq!(snapshot.get::<48>(StorageKey::Array(key_5).as_reference()), None);
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_put_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::<WAL>::new_db_options();
    let mut storage = MVCCStorage::<WAL>::new("storage", &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1], keyspace_id));
    let items: Result<Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<{ BUFFER_VALUE_INLINE }>)>, SnapshotError> =
        snapshot.iterate_range(PrefixRange::new_within(StorageKey::Array(key_prefix))).collect_cloned_vec();
    assert_eq!(items.unwrap(), vec![(key_2, ByteArray::empty()), (key_3, ByteArray::empty()),]);
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_buffered_delete() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::<WAL>::new_db_options();
    let mut storage = MVCCStorage::<WAL>::new("storage", &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x2, 0x0, 0xff], keyspace_id));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    snapshot.delete(key_3.clone());

    assert_eq!(snapshot.get::<48>(StorageKey::Array(key_3).as_reference()), None);

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1], keyspace_id));
    let items: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<{ BUFFER_VALUE_INLINE }>)> =
        snapshot.iterate_range(PrefixRange::new_within(StorageKey::Array(key_prefix))).collect_cloned_vec().unwrap();
    assert_eq!(items, vec![(key_2, ByteArray::empty()),]);
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_read_through() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::<WAL>::new_db_options();
    let mut storage = MVCCStorage::<WAL>::new("storage", &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0x10], keyspace_id));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x0, 0xff], keyspace_id));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x2, 0x0, 0xff], keyspace_id));

    let snapshot = storage.open_snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit().unwrap_or_log();

    let key_5 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1, 0x2, 0x0], keyspace_id));

    // test put - iterate read-through
    let snapshot = storage.open_snapshot_write();
    snapshot.put(key_5.clone());

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x1], keyspace_id));
    let key_values: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<{ BUFFER_VALUE_INLINE }>)> = snapshot
        .iterate_range(PrefixRange::new_within(StorageKey::Array(key_prefix.clone())))
        .collect_cloned_vec()
        .unwrap();
    assert_eq!(
        key_values,
        vec![
            (key_2.clone(), ByteArray::empty()),
            (key_3.clone(), ByteArray::empty()),
            (key_5.clone(), ByteArray::empty()),
        ]
    );

    // test delete-iterate read-through
    snapshot.delete(key_2.clone());
    let key_values: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<{ BUFFER_VALUE_INLINE }>)> =
        snapshot.iterate_range(PrefixRange::new_within(StorageKey::Array(key_prefix))).collect_cloned_vec().unwrap();
    assert_eq!(key_values, vec![(key_3, ByteArray::empty()), (key_5, ByteArray::empty()),]);
    snapshot.close_resources();

    delete_dir(storage_path)
}

#[test]
fn snapshot_delete_reinserted() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::<WAL>::new_db_options();
    let mut storage = MVCCStorage::<WAL>::new("storage", &storage_path).unwrap();
    let keyspace_id: KeyspaceId = 0x0;
    storage.create_keyspace("keyspace", keyspace_id, &options).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((vec![0x0, 0x0, 0x1], keyspace_id));
    let value_0 = ByteArray::copy(&[0, 0, 0, 0]);
    let value_1 = ByteArray::copy(&[0, 0, 0, 1]);

    let snapshot_0 = storage.open_snapshot_write();
    snapshot_0.put_val(key_1.clone(), value_0);
    snapshot_0.commit().unwrap();

    let snapshot_1 = storage.open_snapshot_write();
    snapshot_1.put_val(key_1.clone(), value_1);
    snapshot_1.delete(key_1.clone());
    snapshot_1.commit().unwrap();

    let snapshot_2 = storage.open_snapshot_read();
    assert_eq!(snapshot_2.get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_1).as_reference()), None);
    snapshot_2.close_resources();
    delete_dir(storage_path);
}
