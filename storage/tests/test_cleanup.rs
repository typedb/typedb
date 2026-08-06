/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(const_item_mutation, reason = "`&mut CommitProfile::DISABLED` is a dummy")]

use std::sync::Arc;

use bytes::{Bytes, byte_array::ByteArray, util::HexBytesFormatter};
use diagnostics::metrics::FsyncMetrics;
use durability::wal::WAL;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use logger::result::ResultExt;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::{CommitProfile, StorageCounters},
};
use storage::{
    StorageOpenError,
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError},
    snapshot::{CommittableSnapshot, PreloadedRangesSnapshot, ReadableSnapshot, WritableSnapshot},
};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_storage::{checkpoint_storage, create_storage, load_storage, test_keyspace_set};

use self::TestKeyspaceSet::{Keyspace, Keyspace2};

test_keyspace_set! {
    Keyspace => 0: "keyspace",
    Keyspace2 => 1: "keyspace2",
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

    assert_eq!(
        storage
            .iterate_keyspace_range(
                &IteratorPool::default(),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
                StorageCounters::DISABLED
            )
            .count(),
        4
    );

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.delete(key_1.clone());
    snapshot.delete(key_2.clone());
    let seq = snapshot.commit(&mut CommitProfile::DISABLED).unwrap_or_log().unwrap();

    assert_eq!(
        storage
            .iterate_keyspace_range(
                &IteratorPool::default(),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
                StorageCounters::DISABLED
            )
            .count(),
        6
    );

    storage
        .cleanup_dead_keys(
            seq,
            [(
                StorageKey::new(Keyspace, Bytes::copy(&[])),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
            )],
        )
        .unwrap_or_log();

    assert_eq!(
        storage
            .iterate_keyspace_range(
                &IteratorPool::default(),
                KeyRange::new_unbounded(RangeStart::Inclusive(StorageKey::Array(key_1.clone()))),
                StorageCounters::DISABLED
            )
            .count(),
        2
    );

    let snapshot = storage.open_snapshot_read();
    assert_eq!(
        snapshot.get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_1).as_reference(), StorageCounters::DISABLED).unwrap(),
        None
    );
    assert_eq!(
        snapshot.get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_2).as_reference(), StorageCounters::DISABLED).unwrap(),
        None
    );
    assert!(
        snapshot
            .get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_3).as_reference(), StorageCounters::DISABLED)
            .unwrap()
            .is_some()
    );
    assert!(
        snapshot
            .get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_4).as_reference(), StorageCounters::DISABLED)
            .unwrap()
            .is_some()
    );
}
