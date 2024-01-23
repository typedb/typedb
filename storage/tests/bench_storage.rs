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

use std::cell::OnceCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Mutex;

use logger::initialise_logging;
use tracing::subscriber::DefaultGuard;

use criterion::{Criterion, criterion_group, criterion_main};
use storage::{Section, Storage, SectionId};
use storage::key_value::{Key, KeyFixed, Value};
use storage::snapshot::Snapshot;

static LOGGING_GUARD: Mutex<OnceCell<DefaultGuard>> = Mutex::new(OnceCell::new());

fn setup() -> PathBuf {
    let id = rand::random::<u64>();
    let mut fs_tmp_dir = std::env::temp_dir();
    let dir_name = format!("test_storage_{}", id);
    fs_tmp_dir.push(Path::new(&dir_name));
    fs_tmp_dir
}

fn cleanup(path: PathBuf) {
    std::fs::remove_dir_all(path).ok();
}

fn random_key(section_id: SectionId) -> Key {
    let mut bytes: [u8; 24] = rand::random();
    bytes[0] = 0b0;
    Key::Fixed(KeyFixed::from((bytes.as_slice(), section_id)))
}

fn populate_storage(storage: &Storage, section_id: SectionId, key_count: usize) -> usize {
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
    let prefix = Key::Fixed(KeyFixed::from(([0 as u8].as_slice(), section_id)));
    let iterator = snapshot.iterate_prefix(&prefix);
    iterator.count()
}


fn bench_snapshot_read_get(storage: &Storage, section_id: SectionId) -> Option<Value> {
    let snapshot = Snapshot::Read(storage.snapshot_read());
    let mut last: Option<Value> = None;
    for _ in 0..1 {
        let key = random_key(section_id);
        last = snapshot.get(&key)
    }
    last
}

fn bench_snapshot_write_put(storage: &Storage, section_id: SectionId, batch_size: usize) {
    let snapshot = storage.snapshot_write();
    for _ in 0..batch_size {
        let key = random_key(section_id);
        snapshot.put(key);
    }
    snapshot.commit()
}

fn setup_storage(section_id: SectionId, key_count: usize) -> (Storage, PathBuf) {
    let storage_path = setup();
    let mut storage = Storage::new(Rc::from("storage_bench"), &storage_path).unwrap();
    let options = Section::new_db_options();
    storage.create_section("default", section_id, &options).unwrap();
    let keys = populate_storage(&storage, section_id, key_count);
    println!("Initialised storage with '{}' keys", keys);
    (storage, storage_path)
}

fn criterion_benchmark(c: &mut Criterion) {
    LOGGING_GUARD.lock().unwrap().get_or_init(initialise_logging);
    {
        let section_id = 0 as SectionId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(section_id, initial_key_count);
        c.bench_function("snapshot_read_get", |b| b.iter(|| {
            bench_snapshot_read_get(&storage, section_id)
        }));
        cleanup(storage_path);
    }
    {
        let section_id = 0 as SectionId;
        let initial_key_count: usize = 10_000_000; // approximately 0.2 GB of keys
        let (storage, storage_path) = setup_storage(section_id, initial_key_count);
        c.bench_function("snapshot_write_put", |b| b.iter(|| {
            bench_snapshot_write_put(&storage, section_id, 100)
        }));
        cleanup(storage_path);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);