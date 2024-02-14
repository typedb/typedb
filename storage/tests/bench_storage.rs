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

use criterion::{Criterion, criterion_group, criterion_main};

use bytes::byte_reference::ByteReference;
use storage::key_value::{StorageKey, StorageKeyArray, StorageKeyReference, StorageValueArray};
use storage::keyspace::keyspace::KeyspaceId;
use storage::MVCCStorage;
use storage::snapshot::buffer::{BUFFER_INLINE_KEY, BUFFER_INLINE_VALUE};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

fn random_key_24(keyspace_id: KeyspaceId) -> StorageKeyArray<BUFFER_INLINE_KEY> {
    let mut bytes: [u8; 24] = rand::random();
    bytes[0] = 0b0;
    StorageKeyArray::from((bytes.as_slice(), keyspace_id))
}

fn random_key_4(keyspace_id: KeyspaceId) -> StorageKeyArray<BUFFER_INLINE_KEY> {
    let mut bytes: [u8; 4] = rand::random();
    bytes[0] = 0b0;
    StorageKeyArray::from((bytes.as_slice(), keyspace_id))
}

fn populate_storage(storage: &MVCCStorage, keyspace_id: KeyspaceId, key_count: usize) -> usize {
    const BATCH_SIZE: usize = 1_000;
    let mut snapshot = storage.snapshot_write();
    for i in 0..key_count {
        if i % BATCH_SIZE == 0 {
            snapshot.commit();
            snapshot = storage.snapshot_write();
        }
        snapshot.put(random_key_24(keyspace_id));
    }
    snapshot.commit();
    let snapshot = storage.snapshot_read();
    let prefix = StorageKey::Reference(StorageKeyReference::new(keyspace_id, ByteReference::new(&[0 as u8])));
    let iterator = snapshot.iterate_prefix(&prefix);
    iterator.collect_cloned().len()
}

fn bench_snapshot_read_get(storage: &MVCCStorage, keyspace_id: KeyspaceId) -> Option<StorageValueArray<BUFFER_INLINE_VALUE>> {
    let snapshot = storage.snapshot_read();
    let mut last: Option<StorageValueArray<BUFFER_INLINE_VALUE>> = None;
    for _ in 0..1 {
        let key = random_key_24(keyspace_id);
        last = snapshot.get(&StorageKey::Array(key))
    }
    last
}

fn bench_snapshot_read_iterate<const PREFIX_LEN: usize, const ITERATE_COUNT: usize>(storage: &MVCCStorage, keyspace_id: KeyspaceId) -> Option<StorageValueArray<BUFFER_INLINE_VALUE>> {
    let snapshot = storage.snapshot_read();
    let mut last: Option<StorageValueArray<BUFFER_INLINE_VALUE>> = None;
    for _ in 0..1 {
        let key = random_key_4(keyspace_id);
        last = snapshot.get(&StorageKey::Array(key))
    }
    last
}

fn bench_snapshot_write_put(storage: &MVCCStorage, keyspace_id: KeyspaceId, batch_size: usize) {
    let snapshot = storage.snapshot_write();
    for _ in 0..batch_size {
        let key = random_key_24(keyspace_id);
        snapshot.put(key);
    }
    snapshot.commit()
}

fn setup_storage(keyspace_id: KeyspaceId, key_count: usize) -> (MVCCStorage, PathBuf) {
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage_bench"), &storage_path).unwrap();
    let options = MVCCStorage::new_db_options();
    storage.create_keyspace("default", keyspace_id, &options).unwrap();
    let keys = populate_storage(&storage, keyspace_id, key_count);
    println!("Initialised storage with '{}' keys", keys);
    (storage, storage_path)
}

fn criterion_benchmark(c: &mut Criterion) {
    init_logging();
    {
        let keyspace_id = 1 as KeyspaceId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(keyspace_id, initial_key_count);
        c.bench_function("snapshot_read_get", |b| b.iter(|| {
            bench_snapshot_read_get(&storage, keyspace_id)
        }));
        delete_dir(storage_path);
    }
    {
        let keyspace_id = 0 as KeyspaceId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(keyspace_id, initial_key_count);
        c.bench_function("snapshot_write_put", |b| b.iter(|| {
            bench_snapshot_write_put(&storage, keyspace_id, 100)
        }));
        delete_dir(storage_path);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);