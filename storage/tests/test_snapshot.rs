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
use tracing::dispatcher::DefaultGuard;
use storage::key_value::{Key, KeyFixed, Value};
use storage::Storage;

static LOGGING_GUARD: Mutex<OnceCell<DefaultGuard>> = Mutex::new(OnceCell::new());

fn setup() -> PathBuf {
    LOGGING_GUARD.lock().unwrap().get_or_init(initialise_logging);
    let id = rand::random::<u64>();
    let mut fs_tmp_dir = std::env::temp_dir();
    let dir_name = format!("test_storage_{}", id);
    fs_tmp_dir.push(Path::new(&dir_name));
    fs_tmp_dir
}

fn cleanup(path: PathBuf) {
    std::fs::remove_dir_all(path).ok();
}

#[test]
fn snapshot_buffered_put_get() {
    let storage_path = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_section("sec", sec_id, &storage::Section::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    let value_1 = Box::new([0, 0, 0, 0]);
    snapshot.put_val(key_1.clone(), Value::Value(value_1.clone()));
    snapshot.put(key_2.clone());
    snapshot.put(key_3);
    snapshot.put(key_4);

    assert_eq!(snapshot.get(&key_1), Some(Value::Value(value_1)));
    assert_eq!(snapshot.get(&key_2), Some(Value::Empty));

    let key_5 = Key::Fixed(KeyFixed::from((vec![sec_id, 0xff, 0xff, 0xff], sec_id)));
    assert_eq!(snapshot.get(&key_5), None);

    cleanup(storage_path)
}

#[test]
fn snapshot_buffered_put_iterate() {
    let storage_path = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_section("sec", sec_id, &storage::Section::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    let key_prefix = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, Value)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), Value::Empty),
            (Box::from(key_3.bytes()), Value::Empty),
            (Box::from(key_4.bytes()), Value::Empty),
        ]
    );

    cleanup(storage_path)
}

#[test]
fn snapshot_buffered_delete() {
    let storage_path = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_section("sec", sec_id, &storage::Section::new_db_options()).unwrap();

    let snapshot = storage.snapshot_write();

    let key_1 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));
    snapshot.put(key_1);
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());

    snapshot.delete(key_3.clone());

    assert_eq!(snapshot.get(&key_3), None);

    let key_prefix = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, Value)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), Value::Empty),
            (Box::from(key_4.bytes()), Value::Empty),
        ]
    );

    cleanup(storage_path)
}

#[test]
fn snapshot_read_through() {
    let storage_path = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_id: u8 = 0x0;
    storage.create_section("sec", sec_id, &storage::Section::new_db_options()).unwrap();

    let key_1 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x0, 0x0, 0x1], sec_id)));
    let key_2 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0x10], sec_id)));
    let key_3 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x0, 0xff], sec_id)));
    let key_4 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x2, 0x0, 0xff], sec_id)));

    let snapshot = storage.snapshot_write();
    snapshot.put(key_1.clone());
    snapshot.put(key_2.clone());
    snapshot.put(key_3.clone());
    snapshot.put(key_4.clone());
    snapshot.commit();

    let key_5 = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1, 0x2, 0x0], sec_id)));

    // test put - iterate read-through
    let snapshot = storage.snapshot_write();
    snapshot.put(key_5.clone());

    let key_prefix = Key::Fixed(KeyFixed::from((vec![sec_id, 0x1], sec_id)));
    let key_values: Vec<(Box<[u8]>, Value)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_2.bytes()), Value::Empty),
            (Box::from(key_3.bytes()), Value::Empty),
            (Box::from(key_5.bytes()), Value::Empty),
            (Box::from(key_4.bytes()), Value::Empty),
        ]
    );

    // test delete-iterate read-through
    snapshot.delete(key_2.clone());
    let key_values: Vec<(Box<[u8]>, Value)> = snapshot.iterate_prefix(&key_prefix).collect();
    assert_eq!(
        key_values,
        vec![
            (Box::from(key_3.bytes()), Value::Empty),
            (Box::from(key_5.bytes()), Value::Empty),
            (Box::from(key_4.bytes()), Value::Empty),
        ]
    );

    cleanup(storage_path)
}
