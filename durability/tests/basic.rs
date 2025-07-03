/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use std::{
    fs::{self, read_dir, OpenOptions},
    io::{BufWriter, Seek, Write},
};

use durability::DurabilityService;
use durability_test_common::{create_wal, load_wal, TestRecord};
use itertools::Itertools;
use rand::prelude::*;
use tempdir::TempDir;

#[test]
fn basic() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = create_wal(&directory);
    let written_entry_id = wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap();
    drop(wal);

    let wal = load_wal(&directory);
    let raw_record = wal.iter_any_from(written_entry_id).unwrap().next().unwrap().unwrap();
    let read_record = TestRecord::new(Vec::from(raw_record.bytes));
    assert_eq!(read_record, message);
}

#[test]
fn added_zeros() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = create_wal(&directory);
    let written_entry_id = wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap();
    drop(wal);

    let wal_file = &read_dir(directory.path().join("wal")).unwrap().exactly_one().unwrap().unwrap().path();
    let len = fs::metadata(wal_file).unwrap().len();
    let mut file = BufWriter::new(OpenOptions::new().read(true).append(true).open(wal_file).unwrap());
    file.write_all(&[0; 120]).unwrap();

    let wal = load_wal(&directory);
    let mut wal_iterator = wal.iter_any_from(written_entry_id).unwrap();
    let raw_record = wal_iterator.next().unwrap().unwrap();
    let read_record = TestRecord::new(Vec::from(raw_record.bytes));
    assert_eq!(read_record, message);
    assert!(wal_iterator.next().is_none());
    assert_eq!(fs::metadata(wal_file).unwrap().len(), len)
}

#[test]
fn added_junk() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = create_wal(&directory);
    let written_entry_id = wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap();
    drop(wal);

    let wal_file = &read_dir(directory.path().join("wal")).unwrap().exactly_one().unwrap().unwrap().path();
    let len = fs::metadata(wal_file).unwrap().len();
    let mut file = BufWriter::new(OpenOptions::new().read(true).append(true).open(wal_file).unwrap());
    file.write_all(&thread_rng().gen::<[u8; 32]>()).unwrap();

    let wal = load_wal(&directory);
    let mut wal_iterator = wal.iter_any_from(written_entry_id).unwrap();
    let raw_record = wal_iterator.next().unwrap().unwrap();
    let read_record = TestRecord::new(Vec::from(raw_record.bytes));
    assert_eq!(read_record, message);
    assert!(wal_iterator.next().is_none());
    assert_eq!(fs::metadata(wal_file).unwrap().len(), len)
}

#[test]
fn corrupted_record() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = create_wal(&directory);
    let written_entry_id = wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap();
    drop(wal);

    let wal_file = &read_dir(directory.path().join("wal")).unwrap().exactly_one().unwrap().unwrap().path();
    let mut file = OpenOptions::new().read(true).write(true).open(wal_file).unwrap();
    file.seek(std::io::SeekFrom::End(-16)).unwrap();
    file.write_all(&[0; 16]).unwrap();
    file.sync_all().unwrap();

    let wal = load_wal(&directory);
    assert!(wal.iter_any_from(written_entry_id).unwrap().next().is_none());
    assert_eq!(fs::metadata(wal_file).unwrap().len(), 0)
}
