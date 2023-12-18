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
 *
 */


use std::path::PathBuf;
use std::rc::Rc;

use logger::initialise_logging;
use rand;
use tracing::subscriber::DefaultGuard;

use storage::{error::{StorageError, StorageErrorKind}, key::Key, SectionError, SectionErrorKind, Storage};

fn setup() -> (PathBuf, DefaultGuard) {
    let guard = initialise_logging();
    let id = rand::random::<u64>();
    let fs_tmp_dir = std::env::temp_dir();
    (fs_tmp_dir.with_extension(format!("test_storage_{}", id)), guard)
}

fn cleanup(path: PathBuf) {
    std::fs::remove_dir_all(path).ok();
}

#[test]
fn create_delete() {
    let (storage_path, _log_guard) = setup();
    let storage_result = Storage::new(Rc::from("storage"), &storage_path);
    assert!(storage_result.is_ok());
    let storage = storage_result.unwrap();
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok());
    cleanup(storage_path)
}

#[test]
fn create_sections() {
    let (storage_path, _log_guard) = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_prefix: u8 = 0x0;
    let create_1_result = storage.create_section("sec_1", sec_1_prefix, &storage::Section::new_options());
    assert!(create_1_result.is_ok());
    let sec_2_prefix: u8 = 0x10;
    let create_2_result = storage.create_section("sec_2", sec_2_prefix, &storage::Section::new_options());
    assert!(create_2_result.is_ok(), "{create_2_result:?}");
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok(), "{:?}", delete_result);

    cleanup(storage_path)
}

#[test]
fn create_sections_errors() {
    let (storage_path, _log_guard) = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_prefix: u8 = 0x0;
    storage.create_section("sec_1", sec_1_prefix, &storage::Section::new_options()).unwrap();

    let sec_2_prefix: u8 = 0x10;
    let name_error = storage.create_section("sec_1", sec_2_prefix, &storage::Section::new_options());
    assert!(matches!(name_error, Err(StorageError{
        kind: StorageErrorKind::SectionError {
            source: SectionError {
                kind: SectionErrorKind::FailedToCreateSectionNameExists{..},
                ..
            },
            ..
        },
        ..
    })), "{}", name_error.unwrap_err());

    let duplicate_prefix: u8 = 0x0;
    let prefix_error = storage.create_section("sec_2", duplicate_prefix, &storage::Section::new_options());
    assert!(matches!(prefix_error, Err(StorageError{
        kind: StorageErrorKind::SectionError {
            source: SectionError {
                kind: SectionErrorKind::FailedToCreateSectionPrefixExists{..},
                ..
            },
            ..
        },
        ..
    })), "{}", prefix_error.unwrap_err());

    cleanup(storage_path)
}

#[test]
fn get_put_iterate() {
    let (storage_path, _log_guard) = setup();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_prefix: u8 = 0x0;
    storage.create_section("sec_1", sec_1_prefix, &storage::Section::new_options()).unwrap();
    let sec_2_prefix: u8 = 0x10;
    storage.create_section("sec_2", sec_2_prefix, &storage::Section::new_options()).unwrap();

    let sec_1_key_1 = Key { data: vec![sec_1_prefix, 0x0, 0x0, 0x1] };
    let sec_1_key_2 = Key { data: vec![sec_1_prefix, 0x1, 0x0, 0x10] };
    let sec_1_key_3 = Key { data: vec![sec_1_prefix, 0x1, 0x0, 0xff] };
    let sec_1_key_4 = Key { data: vec![sec_1_prefix, 0x2, 0x0, 0xff] };
    storage.put(&sec_1_key_1);
    storage.put(&sec_1_key_2);
    storage.put(&sec_1_key_3);
    storage.put(&sec_1_key_4);

    let sec_2_key_1 = Key { data: vec![sec_2_prefix, 0x1, 0x0, 0x1] };
    let sec_2_key_2 = Key { data: vec![sec_2_prefix, 0xb, 0x0, 0x10] };
    let sec_2_key_3 = Key { data: vec![sec_2_prefix, 0x5, 0x0, 0xff] };
    let sec_2_key_4 = Key { data: vec![sec_2_prefix, 0x2, 0x0, 0xff] };
    storage.put(&sec_2_key_1);
    storage.put(&sec_2_key_2);
    storage.put(&sec_2_key_3);
    storage.put(&sec_2_key_4);

    let first_value = storage.get(&sec_1_key_1);
    assert!(first_value.is_some() && first_value.unwrap().is_empty());

    let second_value = storage.get(&sec_2_key_1);
    assert!(second_value.is_some() && second_value.unwrap().is_empty());

    let prefix = vec![sec_1_prefix, 0x1];
    let entries: Vec<(Vec<u8>, Vec<u8>)> = storage.iterate_prefix(&prefix)
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    assert_eq!(entries, vec![
        (sec_1_key_2.data, Vec::new()),
        (sec_1_key_3.data, Vec::new()),
        (sec_1_key_4.data, Vec::new()),
    ]);

    cleanup(storage_path)
}
