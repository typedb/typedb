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

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use durability::{DurabilityRecord, DurabilityService, RawRecord};
use tempdir::TempDir;

mod common;

use self::common::{open_wal, TestRecord};

#[test]
fn basic() {
    let directory = TempDir::new("wal-test").unwrap();

    let message = TestRecord { bytes: b"hello world".to_vec() };

    let wal = open_wal(&directory);
    let written_entry_id = wal.sequenced_write(&message).unwrap();
    println!("hello world written to WAL in {written_entry_id}");
    drop(wal);

    let wal = open_wal(&directory);
    let raw_record = wal.iter_from(written_entry_id).unwrap().next().unwrap().unwrap();
    let read_record = TestRecord::deserialize_from(&mut &*raw_record.bytes).unwrap();
    assert_eq!(read_record, message);
    wal.checkpoint().unwrap();
    let written_entry_id = wal.sequenced_write(&message).unwrap();
    println!("hello world written to WAL in {written_entry_id}");
    drop(wal);

    let wal = open_wal(&directory);

    let mut recovery_iterator = wal.recover().unwrap();
    let RawRecord { sequence_number, record_type, bytes } = recovery_iterator.next().unwrap().unwrap();
    assert_eq!(sequence_number, written_entry_id);
    assert_eq!(record_type, TestRecord::RECORD_TYPE);
    assert_eq!(message, TestRecord::deserialize_from(&mut &*bytes).unwrap());
    assert!(recovery_iterator.next().is_none());
    drop(recovery_iterator);

    wal.checkpoint().unwrap();
    drop(wal);

    let wal = open_wal(&directory);
    let mut recovery_iterator = wal.recover().unwrap();
    assert!(recovery_iterator.next().is_none());
}
