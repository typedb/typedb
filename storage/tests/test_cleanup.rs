/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(const_item_mutation, reason = "`&mut CommitProfile::DISABLED` is a dummy")]

use bytes::{Bytes, byte_array::ByteArray};
use lending_iterator::LendingIterator;
use logger::result::ResultExt;
use resource::{
    constants::snapshot::BUFFER_KEY_INLINE,
    profile::{CommitProfile, StorageCounters},
};
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    key_range::{KeyRange, RangeStart},
    key_value::{StorageKey, StorageKeyArray},
    keyspace::IteratorPool,
    snapshot::{CommittableSnapshot, ReadSnapshot, ReadableSnapshot, WritableSnapshot},
};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_storage::{create_storage, test_keyspace_set};

use self::TestKeyspaceSet::Keyspace;

test_keyspace_set! {
    Keyspace => 0: "keyspace",
}

#[test]
fn cleanup_test() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0x10]));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0xff]));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x2, 0x0, 0xff]));

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap_or_log();

    assert_eq!(count_keys(&storage), 4);

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.delete(key_1.clone());
    snapshot.delete(key_2.clone());
    let seq = snapshot.commit(&mut CommitProfile::DISABLED).unwrap_or_log().unwrap();

    assert_eq!(count_keys(&storage), 6);

    storage
        .cleanup_dead_keys(
            seq,
            [(
                StorageKey::new(Keyspace, Bytes::copy(&[])),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
            )],
        )
        .unwrap_or_log();

    assert_eq!(count_keys(&storage), 2);

    let snapshot = storage.open_snapshot_read();
    assert_eq!(get_key(key_1, &snapshot), None);
    assert_eq!(get_key(key_2, &snapshot), None);
    assert!(get_key(key_3, &snapshot).is_some());
    assert!(get_key(key_4, &snapshot).is_some());
}

#[test]
fn concurrent_reader_cleanup_test() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0x10]));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0xff]));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x2, 0x0, 0xff]));

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap_or_log();

    assert_eq!(count_keys(&storage), 4);

    let snapshot = storage.clone().open_snapshot_read();

    let seq = {
        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.delete(key_1.clone());
        snapshot.delete(key_2.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap_or_log().unwrap()
    };

    assert_eq!(count_keys(&storage), 6);

    storage
        .cleanup_dead_keys(
            seq,
            [(
                StorageKey::new(Keyspace, Bytes::copy(&[])),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
            )],
        )
        .unwrap_or_log();

    assert_eq!(count_keys(&storage), 6);

    assert!(get_key(key_1, &snapshot).is_some());
    assert!(get_key(key_2, &snapshot).is_some());
    assert!(get_key(key_3, &snapshot).is_some());
    assert!(get_key(key_4, &snapshot).is_some());
}

fn get_key(
    key: StorageKeyArray<BUFFER_KEY_INLINE>,
    snapshot: &ReadSnapshot<WALClient>,
) -> Option<ByteArray<BUFFER_KEY_INLINE>> {
    snapshot.get::<BUFFER_KEY_INLINE>(StorageKey::Array(key).as_reference(), StorageCounters::DISABLED).unwrap()
}

fn count_keys(storage: &MVCCStorage<WALClient>) -> usize {
    storage
        .iterate_keyspace_range(
            &IteratorPool::default(),
            KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(StorageKeyArray::new(
                Keyspace,
                ByteArray::<BUFFER_KEY_INLINE>::empty(),
            )))),
            StorageCounters::DISABLED,
        )
        .count()
}
