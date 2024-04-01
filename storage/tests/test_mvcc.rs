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

/*
This file should comprise a set of low-level tests relating to MVCC.

1. We should be able to open a new snapshot (check open sequence number) and commit it. The next sequence number should have advanced by one (ie. watermark).

2. We should be able to open a previous snapshot, and doing reads on this version should ignore all newer versions

3. We should be able to open a previous snapshot in write, and the commit should fail if a subsequent commit has a conflicting operation
   Note: edge case is when the commit records have been purged from memory. Isolation manager should be able to retrieve them again from the Durability service on demand.
   Note: let's write this test first, and leave it red. This will induce a refactor of the Timeline - which is the next task.


4. We should be able to configure that a cleanup of old versionsn of key is run after T has elapsed that deletes old versions of data we no longer want to retain
   We want the time keeping component to be received from the durability service, which should be able to tell us the last sequence number
   before a specific time (it should be able to binary search its WAL log files and check the dates on them to find this information).
   After cleanup is run, we should get a good error if a version that is too-old is opened.
   After cleanup is run, if we iterate directly on the storage layer, we should be able to confirm the keys are actually not present anymore (Rocks may defer the disk delete till compaction, but to us they are "gone").

 */

use std::path::Path;

use bytes::{
    byte_array::ByteArray,
    byte_reference::ByteReference
};
use durability::wal::WAL;
use storage::{
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    MVCCStorage,
    KeyspaceSet
};
use test_utils::{create_tmp_dir, init_logging};

macro_rules! test_keyspace_set {
    {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
        #[derive(Copy, Clone)]
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
const VALUE_0: [u8; 1] = [0x0];
const VALUE_1: [u8; 1] = [0x1];
const VALUE_2: [u8; 1] = [0x2];

fn setup_storage(storage_path: &Path) -> MVCCStorage<WAL> {
    let storage = MVCCStorage::recover::<TestKeyspaceSet>("storage", storage_path).unwrap();
    storage
}

#[test]
fn test_commit_increments_watermark() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);
    let wm_initial = storage.read_watermark();
    let snapshot_0 = storage.open_snapshot_write();
    let key_1 = StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_1));
    snapshot_0.put_val(key_1.clone(), ByteArray::copy(&VALUE_1)).unwrap();
    snapshot_0.commit().unwrap();

    assert_eq!(wm_initial.next().number().number() + 1, storage.read_watermark().number().number());
}

#[test]
fn test_reading_snapshots() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);

    let key_1: &StorageKey<'_, 48> = &StorageKey::Reference(StorageKeyReference::new(Keyspace, ByteReference::new(&KEY_1)));

    let snapshot_write_0 = storage.open_snapshot_write();
    snapshot_write_0.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_1)), ByteArray::copy(&VALUE_0)).unwrap();
    snapshot_write_0.commit().unwrap();

    let watermark_0 = storage.read_watermark();

    let snapshot_read_0 = storage.open_snapshot_read();
    assert_eq!(snapshot_read_0.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);

    let snapshot_write_1 = storage.open_snapshot_write();
    snapshot_write_1.put_val(StorageKeyArray::new(Keyspace, ByteArray::copy(&KEY_1)), ByteArray::copy(&VALUE_1)).unwrap();
    let snapshot_read_01 = storage.open_snapshot_read();
    assert_eq!(snapshot_read_0.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);
    assert_eq!(snapshot_read_01.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);

    let result_write_1 = snapshot_write_1.commit();
    assert!(result_write_1.is_ok());

    let snapshot_read_1 = storage.open_snapshot_read();
    assert_eq!(snapshot_read_0.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);
    assert_eq!(snapshot_read_01.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);
    assert_eq!(snapshot_read_1.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_1);
    snapshot_read_1.close_resources();
    snapshot_read_01.close_resources();
    snapshot_read_0.close_resources();

    // Read from further in the past.
    let snapshot_read_02 = storage.open_snapshot_read_at(watermark_0).unwrap();
    assert_eq!(snapshot_read_02.get::<128>(key_1.as_reference()).unwrap().unwrap().bytes(), VALUE_0);
    snapshot_read_02.close_resources();
}

#[test]
fn test_conflicting_update_fails() {
    // TODO: Why does this exist if we have separate isolation tests?
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = setup_storage(&storage_path);

    let key_1 = StorageKey::new_owned(Keyspace, ByteArray::copy(&KEY_1));
    let key_2 = StorageKey::new_owned(Keyspace, ByteArray::copy(&KEY_2));

    let snapshot_write_0 = storage.open_snapshot_write();
    snapshot_write_0.put_val(key_1.clone().into_owned_array(), ByteArray::copy(&VALUE_0)).unwrap();
    snapshot_write_0.commit().unwrap();

    let watermark_after_initial_write = storage.read_watermark();

    {
        let snapshot_write_11 = storage.open_snapshot_write();
        let snapshot_write_21 = storage.open_snapshot_write();
        snapshot_write_11.delete(key_1.clone().into_owned_array());
        snapshot_write_21.get_required(key_1.clone()).unwrap();
        snapshot_write_21.put_val(key_2.clone().into_owned_array(), ByteArray::copy(&VALUE_2)).unwrap();
        let result_write_11 = snapshot_write_11.commit();
        assert!(result_write_11.is_ok());
        let result_write_21 = snapshot_write_21.commit();
        assert!(result_write_21.is_err()); // Fails
    }

    {
        // Try the same, with the snapshot opened in the past
        let snapshot_write_at_0 = storage.open_snapshot_write_at(watermark_after_initial_write).unwrap();
        snapshot_write_at_0.get_required(key_1.clone()).unwrap();
        snapshot_write_at_0.put_val(key_2.clone().into_owned_array(), ByteArray::copy(&VALUE_2)).unwrap();
        let result_write_at_0 = snapshot_write_at_0.commit();
        assert!(result_write_at_0.is_err()); // Fails
    }
}
