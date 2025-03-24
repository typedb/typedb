/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs;

use durability::wal::WAL;
use resource::{
    constants::snapshot::BUFFER_KEY_INLINE,
    profile::{CommitProfile, StorageCounters},
};
use storage::{
    durability_client::WALClient,
    key_value::{StorageKeyArray, StorageKeyReference},
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};
use test_utils_storage::{checkpoint_storage, create_storage, load_storage, test_keyspace_set};

#[test]
fn wal_and_checkpoint_ok() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }

    init_logging();
    let key_hello = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"hello"));
    let key_world = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"world"));

    let storage_path = create_tmp_dir();
    let (checkpoint, watermark) = {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_hello.clone());
        snapshot.put(key_world.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        (checkpoint_storage(&storage), storage.snapshot_watermark())
    };

    {
        let storage =
            load_storage::<TestKeyspaceSet>(&storage_path, WAL::load(&storage_path).unwrap(), Some(checkpoint))
                .unwrap();
        assert_eq!(watermark, storage.snapshot_watermark());
        let snapshot = storage.open_snapshot_read();
        assert!(snapshot
            .get_mapped(StorageKeyReference::from(&key_hello), |_| true, StorageCounters::DISABLED)
            .unwrap()
            .is_some());
    };
}

#[test]
fn wal_missing_records_for_checkpoint_replay_fails() {
    // TODO: test that having a WAL with missing records required to complete the checkpoint, fails
    todo!()
}

#[test]
fn wal_missing_records_entire_replay_fails() {
    // TODO: test that replaying a WAL from scratch fails if any records are missing from the start
    todo!()
}

#[test]
fn wal_and_no_checkpoint_ok() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }

    init_logging();
    let key_hello = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"hello"));
    let key_world = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"world"));

    let storage_path = create_tmp_dir();
    let watermark = {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_hello.clone());
        snapshot.put(key_world.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        storage.snapshot_watermark()
    };

    {
        let storage = load_storage::<TestKeyspaceSet>(&storage_path, WAL::load(&storage_path).unwrap(), None).unwrap();
        assert_eq!(watermark, storage.snapshot_watermark());
        let snapshot = storage.open_snapshot_read();
        assert!(snapshot
            .get_mapped(StorageKeyReference::from(&key_hello), |_| true, StorageCounters::DISABLED)
            .unwrap()
            .is_some());
    }
}

#[test]
fn no_wal_and_checkpoint_illegal() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }

    init_logging();
    let key_hello = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"hello"));
    let key_world = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"world"));

    let storage_path = create_tmp_dir();
    let (_checkpoint, directory) = {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_hello.clone());
        snapshot.put(key_world.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        (checkpoint_storage(&storage), storage.path().parent().unwrap().to_owned())
    };

    // delete wal
    fs::remove_dir_all(directory.join(WAL::WAL_DIR_NAME)).unwrap();

    {
        let wal_result = WAL::load(&storage_path);
        assert!(wal_result.is_err());
    }
}

#[test]
fn no_wal_and_no_checkpoint_and_keyspaces_illegal() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }

    init_logging();
    let key_hello = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"hello"));
    let key_world = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"world"));

    let storage_path = create_tmp_dir();
    {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_hello.clone());
        snapshot.put(key_world.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    };

    // delete wal
    fs::remove_dir_all(storage_path.join(WAL::WAL_DIR_NAME)).unwrap();

    {
        let wal_result = WAL::load(&storage_path);
        assert!(wal_result.is_err());
    }
}

#[test]
fn no_wal_and_no_checkpoint_and_no_keyspaces_illegal() {
    test_keyspace_set! { Keyspace => 0: "keyspace" }

    init_logging();
    let key_hello = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"hello"));
    let key_world = StorageKeyArray::<BUFFER_KEY_INLINE>::from((TestKeyspaceSet::Keyspace, b"world"));

    let storage_path = create_tmp_dir();
    {
        let storage = create_storage::<TestKeyspaceSet>(&storage_path).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        snapshot.put(key_hello.clone());
        snapshot.put(key_world.clone());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    };

    // delete wal
    fs::remove_dir_all(storage_path.join(WAL::WAL_DIR_NAME)).unwrap();
    // delete keyspaces
    fs::remove_dir_all(storage_path.join(MVCCStorage::<WALClient>::STORAGE_DIR_NAME)).unwrap();

    {
        let wal_result = WAL::load(&storage_path);
        assert!(wal_result.is_err());
    }
}
