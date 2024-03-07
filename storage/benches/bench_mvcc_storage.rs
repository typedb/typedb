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

use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;
use storage::key_value::{StorageKey, StorageKeyArray, StorageKeyReference};
use storage::keyspace::keyspace::KeyspaceId;
use storage::MVCCStorage;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

fn random_key_24(keyspace_id: KeyspaceId) -> StorageKeyArray<{ BUFFER_KEY_INLINE }> {
    let mut bytes: [u8; 24] = rand::random();
    bytes[0] = 0b0;
    StorageKeyArray::from((bytes.as_slice(), keyspace_id))
}

fn random_key_4(keyspace_id: KeyspaceId) -> StorageKeyArray<{ BUFFER_KEY_INLINE }> {
    let mut bytes: [u8; 4] = rand::random();
    bytes[0] = 0b0;
    StorageKeyArray::from((bytes.as_slice(), keyspace_id))
}

fn populate_storage(storage: &MVCCStorage, keyspace_id: KeyspaceId, key_count: usize) -> usize {
    const BATCH_SIZE: usize = 1_000;
    let mut snapshot = storage.open_snapshot_write();
    for i in 0..key_count {
        if i % BATCH_SIZE == 0 {
            snapshot.commit();
            snapshot = storage.open_snapshot_write();
        }
        snapshot.put(random_key_24(keyspace_id));
    }
    snapshot.commit();
    println!("Keys written: {}", key_count);
    let snapshot = storage.open_snapshot_read();
    let prefix: StorageKey<'_, 48> = StorageKey::Reference(StorageKeyReference::new(keyspace_id, ByteReference::new(&[0 as u8])));
    let iterator = snapshot.iterate_prefix(prefix);
    let count = iterator.collect_cloned().len();
    println!("Keys confirmed to be written: {}", count);
    count
}

fn bench_snapshot_read_get(storage: &MVCCStorage, keyspace_id: KeyspaceId) -> Option<ByteArray<{ BUFFER_VALUE_INLINE }>> {
    let snapshot = storage.open_snapshot_read();
    let mut last: Option<ByteArray<{BUFFER_VALUE_INLINE}>> = None;
    for _ in 0..1 {
        last = snapshot.get(StorageKey::Array(random_key_24(keyspace_id)).as_reference());
    }
    last
}

fn bench_snapshot_read_iterate<const ITERATE_COUNT: usize>(storage: &MVCCStorage, keyspace_id: KeyspaceId) -> Option<ByteArray<{ BUFFER_VALUE_INLINE }>> {
    let snapshot = storage.open_snapshot_read();
    let mut last: Option<ByteArray<{BUFFER_VALUE_INLINE}>> = None;
    for _ in 0..ITERATE_COUNT {
        last = snapshot.get(StorageKey::Array(random_key_4(keyspace_id)).as_reference())
    }
    last
}

fn bench_snapshot_write_put(storage: &MVCCStorage, keyspace_id: KeyspaceId, batch_size: usize) {
    let snapshot = storage.open_snapshot_write();
    for _ in 0..batch_size {
        snapshot.put(random_key_24(keyspace_id));
    }
    snapshot.commit().unwrap()
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
    const INITIAL_KEY_COUNT: usize = 10_000; // 10 million = approximately 0.2 GB of keys
    const KEYSPACE_ID: KeyspaceId = 0;
    {
        let (storage, storage_path) = setup_storage(KEYSPACE_ID, INITIAL_KEY_COUNT);
        c.bench_function("snapshot_read_get", |b| b.iter(|| {
            bench_snapshot_read_get(&storage, KEYSPACE_ID)
        }));
        delete_dir(storage_path);
    }
    {
        let (storage, storage_path) = setup_storage(KEYSPACE_ID, INITIAL_KEY_COUNT);
        c.bench_function("snapshot_write_put", |b| b.iter(|| {
            bench_snapshot_write_put(&storage, KEYSPACE_ID, 100)
        }));
        delete_dir(storage_path);
    }
    {
        let (storage, storage_path) = setup_storage(KEYSPACE_ID, INITIAL_KEY_COUNT);
        c.bench_function("snapshot_read_iterate", |b| b.iter(|| {
            bench_snapshot_read_iterate::<1>(&storage, KEYSPACE_ID)
        }));
        delete_dir(storage_path);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);


// --- TODO: this flame graph output isn't working. Copied from https://www.jibbow.com/posts/criterion-flamegraphs/ ---

// pub struct FlamegraphProfiler<'a> {
//     frequency: c_int,
//     active_profiler: Option<ProfilerGuard<'a>>,
// }
//
// impl<'a> FlamegraphProfiler<'a> {
//     #[allow(dead_code)]
//     pub fn new(frequency: c_int) -> Self {
//         FlamegraphProfiler {
//             frequency,
//             active_profiler: None,
//         }
//     }
// }
//
// impl<'a> Profiler for FlamegraphProfiler<'a> {
//     fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
//         self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
//     }
//
//     fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
//         std::fs::create_dir_all(benchmark_dir).unwrap();
//         let flamegraph_path = benchmark_dir.join("flamegraph.svg");
//         let flamegraph_file = File::create(&flamegraph_path)
//             .expect("File system error while creating flamegraph.svg");
//         if let Some(profiler) = self.active_profiler.take() {
//             profiler
//                 .report()
//                 .build()
//                 .unwrap()
//                 .flamegraph(flamegraph_file)
//                 .expect("Error writing flamegraph");
//         }
//     }
// }
//
// fn profiled() -> Criterion {
//     Criterion::default().with_profiler(FlamegraphProfiler::new(100))
// }
// criterion_group!(
//     name = benches;
//     config = profiled();
//     targets = criterion_benchmark
// );
//
// criterion_main!(benches);
