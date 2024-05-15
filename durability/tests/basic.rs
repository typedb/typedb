/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use durability::DurabilityService;
use durability_test_common::{create_wal, load_wal, TestRecord};
use tempdir::TempDir;

#[test]
fn basic() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = create_wal(&directory);
    let written_entry_id = wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap();
    println!("hello world written to WAL in {written_entry_id}");
    drop(wal);

    let wal = load_wal(&directory);
    let raw_record = wal.iter_any_from(written_entry_id).unwrap().next().unwrap().unwrap();
    let read_record = TestRecord::new(Vec::from(raw_record.bytes));
    assert_eq!(read_record, message);
}
