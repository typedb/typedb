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
use storage::{StorageSection, KeyspaceId, MVCCStorage};
use storage::key_value::{KeyspaceKey, SectionKeyFixed, Value};
use storage::snapshot::Snapshot;
use test_utils::{init_logging, create_tmp_dir, delete_dir};

fn random_key(section_id: KeyspaceId) -> KeyspaceKey {
    let mut bytes: [u8; 24] = rand::random();
    bytes[0] = 0b0;
    KeyspaceKey::Fixed(SectionKeyFixed::from((bytes.as_slice(), section_id)))
}

fn populate_storage(storage: &MVCCStorage, section_id: KeyspaceId, key_count: usize) -> usize {
    const BATCH_SIZE: usize = 1_000;
    let mut snapshot = storage.snapshot_write();
    for i in 0..key_count {
        if i % BATCH_SIZE == 0 {
            snapshot.commit();
            snapshot = storage.snapshot_write();
        }
        snapshot.put(random_key(section_id));
    }
    snapshot.commit();
    let snapshot = Snapshot::Read(storage.snapshot_read());
    let prefix = KeyspaceKey::Fixed(SectionKeyFixed::from(([0 as u8].as_slice(), section_id)));
    let iterator = snapshot.iterate_prefix(&prefix);
    iterator.count()
}


fn bench_snapshot_read_get(storage: &MVCCStorage, section_id: KeyspaceId) -> Option<Value> {
    let snapshot = Snapshot::Read(storage.snapshot_read());
    let mut last: Option<Value> = None;
    for _ in 0..1 {
        let key = random_key(section_id);
        last = snapshot.get(&key)
    }
    last
}

fn bench_snapshot_write_put(storage: &MVCCStorage, section_id: KeyspaceId, batch_size: usize) {
    let snapshot = storage.snapshot_write();
    for _ in 0..batch_size {
        let key = random_key(section_id);
        snapshot.put(key);
    }
    snapshot.commit()
}

fn setup_storage(section_id: KeyspaceId, key_count: usize) -> (MVCCStorage, PathBuf) {
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage_bench"), &storage_path).unwrap();
    let options = StorageSection::new_db_options();
    storage.create_keyspace("default", section_id, &options).unwrap();
    let keys = populate_storage(&storage, section_id, key_count);
    println!("Initialised storage with '{}' keys", keys);
    (storage, storage_path)
}

fn criterion_benchmark(c: &mut Criterion) {
    init_logging();
    {
        let section_id = 0 as KeyspaceId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(section_id, initial_key_count);
        c.bench_function("snapshot_read_get", |b| b.iter(|| {
            bench_snapshot_read_get(&storage, section_id)
        }));
        delete_dir(storage_path);
    }
    {
        let section_id = 0 as KeyspaceId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(section_id, initial_key_count);
        c.bench_function("snapshot_write_put", |b| b.iter(|| {
            bench_snapshot_write_put(&storage, section_id, 100)
        }));
        delete_dir(storage_path);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);