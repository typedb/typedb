/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use bytes::byte_array::ByteArray;
use lending_iterator::LendingIterator;
use logger::result::ResultExt;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::{CommitProfile, StorageCounters},
};
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray},
    keyspace::KeyspaceSet,
    snapshot::{CommittableSnapshot, PreloadedRangesSnapshot, ReadableSnapshot, WritableSnapshot},
};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_storage::{create_storage, test_keyspace_set};

use self::TestKeyspaceSet::{Keyspace, Keyspace2};

test_keyspace_set! {
    Keyspace => 0: "keyspace",
    Keyspace2 => 1: "keyspace2",
}

#[test]
fn snapshot_generated_new_id() {
    init_logging();
    let mut profile = CommitProfile::disabled();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let mut snapshot1 = storage.clone().open_snapshot_write();
    let mut snapshot2 = storage.clone().open_snapshot_write();
    assert_eq!(snapshot1.open_sequence_number(), snapshot2.open_sequence_number());
    assert_ne!(snapshot1.id(), snapshot2.id());

    let snapshot1_open_seq_num = snapshot1.open_sequence_number();
    let snapshot2_id = snapshot2.id();
    snapshot1.put(StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1])));
    let seqnum1 = snapshot1.commit(&mut profile).unwrap().unwrap().sequence_number;

    let snapshot3 = storage.clone().open_snapshot_write();
    assert_ne!(snapshot3.open_sequence_number(), snapshot1_open_seq_num);
    assert_ne!(snapshot3.open_sequence_number(), snapshot2.open_sequence_number());
    assert_eq!(snapshot3.open_sequence_number(), seqnum1);
    assert_ne!(snapshot3.id(), snapshot2_id);

    let snapshot4 = storage.clone().open_snapshot_schema();
    assert_eq!(snapshot4.open_sequence_number(), seqnum1);
    assert_ne!(snapshot4.id(), snapshot3.id());
}

#[test]
fn snapshot_buffered_put_get() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let mut snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0x10]));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0xff]));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x2, 0x0, 0xff]));
    let value_1 = ByteArray::copy(&[0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), value_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(
        snapshot.get(StorageKey::Array(key_1).as_reference(), StorageCounters::DISABLED).unwrap(),
        Some(value_1)
    );
    assert_eq!(
        snapshot.get::<48>(StorageKey::Array(key_2).as_reference(), StorageCounters::DISABLED).unwrap(),
        Some(ByteArray::empty())
    );

    let key_5 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0xff, 0xff, 0xff]));
    assert_eq!(snapshot.get::<48>(StorageKey::Array(key_5).as_reference(), StorageCounters::DISABLED).unwrap(), None);
}

#[test]
fn snapshot_buffered_put_iterate() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let mut snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0x10]));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0xff]));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x2, 0x0, 0xff]));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1]));
    let items: Result<Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)>, _> = snapshot
        .iterate_range(&KeyRange::new_within(StorageKey::Array(key_prefix), false), StorageCounters::DISABLED)
        .collect_cloned_vec(|k, v| (StorageKeyArray::from(k), ByteArray::from(v)));
    assert_eq!(items.unwrap(), vec![(key_2, ByteArray::empty()), (key_3, ByteArray::empty())]);
}

#[test]
fn snapshot_buffered_delete() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let mut snapshot = storage.open_snapshot_write();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0x10]));
    let key_3 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x0, 0xff]));
    let key_4 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x2, 0x0, 0xff]));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    snapshot.delete(key_3.clone());

    assert_eq!(snapshot.get::<48>(StorageKey::Array(key_3).as_reference(), StorageCounters::DISABLED).unwrap(), None);

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1]));
    let items: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)> = snapshot
        .iterate_range(&KeyRange::new_within(StorageKey::Array(key_prefix), false), StorageCounters::DISABLED)
        .collect_cloned_vec(|k, v| (StorageKeyArray::from(k), ByteArray::from(v)))
        .unwrap();
    assert_eq!(items, vec![(key_2, ByteArray::empty())]);
}

#[test]
fn snapshot_read_through() {
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
    snapshot.commit(&mut CommitProfile::disabled()).unwrap_or_log();

    let key_5 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1, 0x2, 0x0]));

    // test put - iterate read-through
    let mut snapshot = storage.open_snapshot_write();
    snapshot.put(key_5.clone());

    let key_prefix = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x1]));
    let key_values: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)> = snapshot
        .iterate_range(&KeyRange::new_within(StorageKey::Array(key_prefix.clone()), false), StorageCounters::DISABLED)
        .collect_cloned_vec(|k, v| (StorageKeyArray::from(k), ByteArray::from(v)))
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
    let key_values: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)> = snapshot
        .iterate_range(&KeyRange::new_within(StorageKey::Array(key_prefix), false), StorageCounters::DISABLED)
        .collect_cloned_vec(|k, v| (StorageKeyArray::from(k), ByteArray::from(v)))
        .unwrap();
    assert_eq!(key_values, vec![(key_3, ByteArray::empty()), (key_5, ByteArray::empty())]);
}

#[test]
fn snapshot_read_buffered_delete_of_persisted_key() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0]));
    let key_2 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x1]));
    {
        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_1.clone());
        snapshot.put(key_2.clone());
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    }

    {
        let mut snapshot = storage.clone().open_snapshot_write();
        assert!(
            snapshot
                .get::<48>(StorageKey::Array(key_1.clone()).as_reference(), StorageCounters::DISABLED)
                .unwrap()
                .is_some()
        );
        assert!(
            snapshot
                .get::<48>(StorageKey::Array(key_2.clone()).as_reference(), StorageCounters::DISABLED)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            2,
            snapshot
                .iterate_range(
                    &KeyRange::new_within(
                        StorageKey::Array(StorageKeyArray::new(Keyspace, ByteArray::inline([0x0], 1))),
                        false
                    ),
                    StorageCounters::DISABLED
                )
                .count()
        );
        snapshot.delete(key_2.clone());
        assert!(
            snapshot
                .get::<48>(StorageKey::Array(key_2.clone()).as_reference(), StorageCounters::DISABLED)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            1,
            snapshot
                .iterate_range(
                    &KeyRange::new_within(
                        StorageKey::Array(StorageKeyArray::new(Keyspace, ByteArray::inline([0x0], 1))),
                        false
                    ),
                    StorageCounters::DISABLED
                )
                .count()
        );
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    }
}

#[test]
fn snapshot_delete_reinserted() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let key_1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x0, 0x0, 0x1]));
    let value_0 = ByteArray::copy(&[0, 0, 0, 0]);
    let value_1 = ByteArray::copy(&[0, 0, 0, 1]);

    let mut snapshot_0 = storage.clone().open_snapshot_write();
    snapshot_0.put_val(key_1.clone(), value_0);
    snapshot_0.commit(&mut CommitProfile::disabled()).unwrap();

    let mut snapshot_1 = storage.clone().open_snapshot_write();
    snapshot_1.put_val(key_1.clone(), value_1);
    snapshot_1.delete(key_1.clone());
    snapshot_1.commit(&mut CommitProfile::disabled()).unwrap();

    let snapshot_2 = storage.open_snapshot_read();
    assert_eq!(
        snapshot_2
            .get::<BUFFER_KEY_INLINE>(StorageKey::Array(key_1).as_reference(), StorageCounters::DISABLED)
            .unwrap(),
        None
    );
}

#[test]
fn snapshot_iterate_versions_deletes_buffered_and_seek() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();
    let key = |i: u8| StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x5, 0x0, i]));
    let value = |i: u8, version: u8| ByteArray::<BUFFER_VALUE_INLINE>::copy(&[i, version]);
    let prefix = || KeyRange::new_within(StorageKey::Array(StorageKeyArray::from((Keyspace, [0x5]))), true);
    let collect = |snapshot: &dyn Fn() -> Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)>| {
        snapshot().into_iter().map(|(key, value)| (key.bytes()[2], value.to_vec())).collect::<Vec<_>>()
    };
    let expected = |entries: &[(u8, u8)]| entries.iter().map(|&(i, version)| (i, vec![i, version])).collect::<Vec<_>>();

    let mut snapshot = storage.clone().open_snapshot_write();
    (0..10).for_each(|i| snapshot.put_val(key(i), value(i, 0)));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    let old = storage.clone().open_snapshot_read();

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.put_val(key(1), value(1, 1));
    snapshot.delete(key(2));
    snapshot.delete(key(5));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    let middle = storage.clone().open_snapshot_read();

    let mut snapshot = storage.clone().open_snapshot_write();
    snapshot.put_val(key(5), value(5, 2));
    snapshot.put_val(key(10), value(10, 2));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    let new = storage.clone().open_snapshot_read();

    // every snapshot only sees its own versions: newer versions are skipped, as are older versions and deleted keys
    let all_old: Vec<_> = (0..10).map(|i| (i, 0)).collect();
    assert_eq!(collect(&|| collect_range(&old, &prefix())), expected(&all_old));
    assert_eq!(
        collect(&|| collect_range(&middle, &prefix())),
        expected(&[(0, 0), (1, 1), (3, 0), (4, 0), (6, 0), (7, 0), (8, 0), (9, 0)])
    );
    let all_new = [(0, 0), (1, 1), (3, 0), (4, 0), (5, 2), (6, 0), (7, 0), (8, 0), (9, 0), (10, 2)];
    assert_eq!(collect(&|| collect_range(&new, &prefix())), expected(&all_new));
    for i in 0..=10 {
        let get = new.get::<BUFFER_VALUE_INLINE>(StorageKey::Array(key(i)).as_reference(), StorageCounters::DISABLED);
        let expected = all_new.iter().find(|(j, _)| *j == i).map(|&(i, version)| vec![i, version]);
        assert_eq!(get.unwrap().map(|value| value.to_vec()), expected, "get {i}");
    }

    // seeks only move forward: to an absent key, to the current key, and backwards (a no-op)
    let mut iterator = new.iterate_range(&prefix(), StorageCounters::DISABLED);
    let next_index = |iterator: &mut storage::snapshot::iterator::SnapshotRangeIterator| {
        iterator.next().map(|item| item.unwrap().0.bytes()[2])
    };
    assert_eq!(iterator.peek().unwrap().unwrap().0.bytes()[2], 0);
    iterator.seek(StorageKey::Array(key(2)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(3));
    iterator.seek(StorageKey::Array(key(4)).as_reference());
    assert_eq!(iterator.peek().unwrap().unwrap().0.bytes()[2], 4);
    iterator.seek(StorageKey::Array(key(4)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(4));
    iterator.seek(StorageKey::Array(key(1)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(5));
    iterator.seek(StorageKey::Array(key(9)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(9));
    assert_eq!(next_index(&mut iterator), Some(10));
    assert_eq!(next_index(&mut iterator), None);
    assert!(iterator.peek().is_none());

    // buffered writes merge with storage: a re-insert of a deleted key, a delete, an overwrite, and a new key
    let mut write = storage.clone().open_snapshot_write();
    write.put_val(key(2), value(2, 3));
    write.delete(key(3));
    write.put_val(key(4), value(4, 3));
    write.put_val(key(11), value(11, 3));
    assert_eq!(
        collect(&|| collect_range(&write, &prefix())),
        expected(&[(0, 0), (1, 1), (2, 3), (4, 3), (5, 2), (6, 0), (7, 0), (8, 0), (9, 0), (10, 2), (11, 3)])
    );
    let mut iterator = write.iterate_range(&prefix(), StorageCounters::DISABLED);
    iterator.seek(StorageKey::Array(key(3)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(4));
    iterator.seek(StorageKey::Array(key(10)).as_reference());
    assert_eq!(next_index(&mut iterator), Some(10));
    assert_eq!(next_index(&mut iterator), Some(11));
    assert_eq!(next_index(&mut iterator), None);
}

// ---------------------------------------------------------------------------
// PreloadedRangesSnapshot
// ---------------------------------------------------------------------------

fn collect_range<S: ReadableSnapshot>(
    snapshot: &S,
    range: &KeyRange<StorageKey<'_, BUFFER_KEY_INLINE>>,
) -> Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)> {
    snapshot
        .iterate_range(range, StorageCounters::DISABLED)
        .collect_cloned_vec(|k, v| (StorageKeyArray::from(k), ByteArray::from(v)))
        .unwrap()
}

#[test]
fn preloaded_snapshot_matches_source_over_mixed_writes() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let committed_key = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x10, 0xAA]));
    let committed_then_deleted = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x10, 0xBB]));
    let val_committed = ByteArray::copy(&[1, 1]);
    {
        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put_val(committed_key.clone(), val_committed.clone());
        snapshot.put_val(committed_then_deleted.clone(), ByteArray::copy(&[9, 9]));
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    }

    let buffered_put = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x10, 0xCC]));
    let buffered_insert = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x11, 0x00]));
    let out_of_range_key = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x20, 0xDD]));
    let val_buffered = ByteArray::copy(&[2, 2]);

    let mut source = storage.clone().open_snapshot_write();
    source.put_val(buffered_put.clone(), val_buffered.clone());
    source.insert_val(buffered_insert.clone(), ByteArray::copy(&[3, 3]));
    source.delete(committed_then_deleted.clone());
    source.put(out_of_range_key.clone());

    // Materialise the keyspace, limiting the leading byte to {0x10, 0x11}. Keys at
    // 0x20 should be excluded by the load-time scan.
    let preloaded = PreloadedRangesSnapshot::load_from(&source, vec![(Keyspace.id(), vec![0x10..=0x11])]).unwrap();

    // get: present keys hit, tombstoned + out-of-range miss.
    assert_eq!(
        preloaded
            .get::<BUFFER_VALUE_INLINE>(
                StorageKey::Array(committed_key.clone()).as_reference(),
                StorageCounters::DISABLED
            )
            .unwrap(),
        Some(val_committed.clone())
    );
    assert_eq!(
        preloaded
            .get::<BUFFER_VALUE_INLINE>(
                StorageKey::Array(buffered_put.clone()).as_reference(),
                StorageCounters::DISABLED
            )
            .unwrap(),
        Some(val_buffered.clone())
    );
    assert_eq!(
        preloaded
            .get::<BUFFER_VALUE_INLINE>(
                StorageKey::Array(committed_then_deleted.clone()).as_reference(),
                StorageCounters::DISABLED,
            )
            .unwrap(),
        None,
    );

    // iterate_range over [0x10..=0x11] inclusive matches the source's view of the
    // same range, minus the tombstoned committed_then_deleted.
    let range_start = StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x10])));
    let range_end = StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x11])));
    let key_range = KeyRange::new_variable_width(
        RangeStart::Inclusive(range_start.clone()),
        RangeEnd::EndPrefixInclusive(range_end),
    );
    let source_view = collect_range(&source, &key_range);
    let preloaded_view = collect_range(&preloaded, &key_range);
    assert_eq!(source_view, preloaded_view);
    assert_eq!(
        preloaded_view,
        vec![(committed_key, val_committed), (buffered_put, val_buffered), (buffered_insert, ByteArray::copy(&[3, 3])),],
    );

    // any_in_range over a range fully covered by the preloaded set.
    assert!(preloaded.any_in_range(&key_range, false));

    // any_in_range over an empty sub-range: no keys live between 0x10..0x10/0x00
    // exclusive of 0x10/0xAA, so the buffered-only iterator should report none.
    let empty_range = KeyRange::new_variable_width(
        RangeStart::Inclusive(StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x10, 0x00])))),
        RangeEnd::EndPrefixExclusive(StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((
            Keyspace,
            [0x10, 0xAA],
        )))),
    );
    assert!(!preloaded.any_in_range(&empty_range, false));
}

#[test]
fn preloaded_snapshot_multi_keyspace() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let k1_a = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x05, 0x01]));
    let k1_b = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x05, 0x02]));
    let k2_a = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace2, [0x05, 0x01]));
    let k2_b = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace2, [0x05, 0x02]));

    let mut source = storage.clone().open_snapshot_write();
    source.put_val(k1_a.clone(), ByteArray::copy(&[1]));
    source.put_val(k1_b.clone(), ByteArray::copy(&[2]));
    source.put_val(k2_a.clone(), ByteArray::copy(&[3]));
    source.put_val(k2_b.clone(), ByteArray::copy(&[4]));

    let preloaded = PreloadedRangesSnapshot::load_from(
        &source,
        vec![(Keyspace.id(), vec![0x05..=0x05]), (Keyspace2.id(), vec![0x05..=0x05])],
    )
    .unwrap();

    let range_for = |ks: TestKeyspaceSet| {
        KeyRange::new_within(StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((ks, [0x05]))), false)
    };
    assert_eq!(
        collect_range(&preloaded, &range_for(Keyspace)),
        vec![(k1_a.clone(), ByteArray::copy(&[1])), (k1_b.clone(), ByteArray::copy(&[2]))],
    );
    assert_eq!(
        collect_range(&preloaded, &range_for(Keyspace2)),
        vec![(k2_a.clone(), ByteArray::copy(&[3])), (k2_b.clone(), ByteArray::copy(&[4]))],
    );

    assert_eq!(
        preloaded
            .get::<BUFFER_VALUE_INLINE>(StorageKey::Array(k1_a).as_reference(), StorageCounters::DISABLED)
            .unwrap(),
        Some(ByteArray::copy(&[1]))
    );
    assert_eq!(
        preloaded
            .get::<BUFFER_VALUE_INLINE>(StorageKey::Array(k2_a).as_reference(), StorageCounters::DISABLED)
            .unwrap(),
        Some(ByteArray::copy(&[3]))
    );
}

#[test]
fn preloaded_snapshot_load_from_at_sequence_number() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

    let key_at_t0 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x07, 0x01]));
    let key_at_t1 = StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x07, 0x02]));

    let mut snap_t0 = storage.clone().open_snapshot_write();
    snap_t0.put_val(key_at_t0.clone(), ByteArray::copy(&[0]));
    let seq_t0 = snap_t0.commit(&mut CommitProfile::disabled()).unwrap().unwrap().sequence_number;

    let mut snap_t1 = storage.clone().open_snapshot_write();
    snap_t1.put_val(key_at_t1.clone(), ByteArray::copy(&[1]));
    snap_t1.commit(&mut CommitProfile::disabled()).unwrap();

    let preloaded = PreloadedRangesSnapshot::load_from(
        &storage.clone().open_snapshot_read_at(seq_t0),
        vec![(Keyspace.id(), vec![0x07..=0x07])],
    )
    .unwrap();

    let key_range =
        KeyRange::new_within(StorageKey::Array(StorageKeyArray::<BUFFER_KEY_INLINE>::from((Keyspace, [0x07]))), false);
    assert_eq!(collect_range(&preloaded, &key_range), vec![(key_at_t0, ByteArray::copy(&[0]))]);
}
