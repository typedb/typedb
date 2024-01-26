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


use std::rc::Rc;

use rand;
use storage::{error::{StorageError, StorageErrorKind}, SectionError, SectionErrorKind, Storage};
use storage::key_value::{Key, KeyFixed, Value};
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn create_delete() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage_result = Storage::new(Rc::from("storage"), &storage_path);
    assert!(storage_result.is_ok());
    let storage = storage_result.unwrap();
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok());
    delete_dir(storage_path)
}

#[test]
fn create_sections() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_prefix: u8 = 0x0;
    let create_1_result = storage.create_section("sec_1", sec_1_prefix, &storage::Section::new_db_options());
    assert!(create_1_result.is_ok());
    let sec_2_prefix: u8 = 0x10;
    let create_2_result = storage.create_section("sec_2", sec_2_prefix, &storage::Section::new_db_options());
    assert!(create_2_result.is_ok(), "{create_2_result:?}");
    let delete_result = storage.delete_storage();
    assert!(delete_result.is_ok(), "{:?}", delete_result);

    delete_dir(storage_path)
}

#[test]
fn create_sections_errors() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_prefix: u8 = 0x0;
    storage.create_section("sec_1", sec_1_prefix, &storage::Section::new_db_options()).unwrap();

    let sec_2_prefix: u8 = 0x10;
    let name_error = storage.create_section("sec_1", sec_2_prefix, &storage::Section::new_db_options());
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
    let prefix_error = storage.create_section("sec_2", duplicate_prefix, &storage::Section::new_db_options());
    assert!(matches!(prefix_error, Err(StorageError{
        kind: StorageErrorKind::SectionError {
            source: SectionError {
                kind: SectionErrorKind::FailedToCreateSectionIDExists{..},
                ..
            },
            ..
        },
        ..
    })), "{}", prefix_error.unwrap_err());

    delete_dir(storage_path)
}

#[test]
fn get_put_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = Storage::new(Rc::from("storage"), &storage_path).unwrap();
    let sec_1_id: u8 = 0x0;
    storage.create_section("sec_1", sec_1_id, &storage::Section::new_db_options()).unwrap();
    let sec_2_id: u8 = 0x10;
    storage.create_section("sec_2", sec_2_id, &storage::Section::new_db_options()).unwrap();

    let sec_1_key_1 = Key::Fixed(KeyFixed::from((vec![sec_1_id, 0x0, 0x0, 0x1], sec_1_id)));
    let sec_1_key_2 = Key::Fixed(KeyFixed::from((vec![sec_1_id, 0x1, 0x0, 0x10], sec_1_id)));
    let sec_1_key_3 = Key::Fixed(KeyFixed::from((vec![sec_1_id, 0x1, 0x0, 0xff], sec_1_id)));
    let sec_1_key_4 = Key::Fixed(KeyFixed::from((vec![sec_1_id, 0x2, 0x0, 0xff], sec_1_id)));
    storage.put(&sec_1_key_1, &Value::Empty);
    storage.put(&sec_1_key_2, &Value::Empty);
    storage.put(&sec_1_key_3, &Value::Empty);
    storage.put(&sec_1_key_4, &Value::Empty);

    let sec_2_key_1 = Key::Fixed(KeyFixed::from((vec![sec_2_id, 0x1, 0x0, 0x1], sec_2_id)));
    let sec_2_key_2 = Key::Fixed(KeyFixed::from((vec![sec_2_id, 0xb, 0x0, 0x10], sec_2_id)));
    let sec_2_key_3 = Key::Fixed(KeyFixed::from((vec![sec_2_id, 0x5, 0x0, 0xff], sec_2_id)));
    let sec_2_key_4 = Key::Fixed(KeyFixed::from((vec![sec_2_id, 0x2, 0x0, 0xff], sec_2_id)));
    storage.put(&sec_2_key_1, &Value::Empty);
    storage.put(&sec_2_key_2, &Value::Empty);
    storage.put(&sec_2_key_3, &Value::Empty);
    storage.put(&sec_2_key_4, &Value::Empty);

    let first_value = storage.get(&sec_1_key_1);
    assert_eq!(first_value, Some(Value::Empty));

    let second_value = storage.get(&sec_2_key_1);
    assert_eq!(second_value, Some(Value::Empty));

    let prefix = Key::Fixed(KeyFixed::from((vec![sec_1_id, 0x1], sec_1_id)));
    let entries: Vec<(Vec<u8>, Value)> = storage.iterate_prefix(&prefix)
        .map(|(key, value)| (key.to_vec(), value))
        .collect();
    assert_eq!(entries, vec![
        (sec_1_key_2.bytes().to_vec(), Value::Empty),
        (sec_1_key_3.bytes().to_vec(), Value::Empty),
        (sec_1_key_4.bytes().to_vec(), Value::Empty),
    ]);

    delete_dir(storage_path)
}

