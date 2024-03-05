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

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use primitive::U80;

use crate::{DurabilityError, DurabilityErrorKind, DurabilityRecord, DurabilityRecordType, DurabilityService, SequenceNumber, Sequencer};

///
/// I think we could use an MMAP append-only file to allow records to serialise themselves directly into the right place
/// We could also use a Writer/Stream compressor to reduce the write bandwidth requirements
///
#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, &'static str>,
    sequence_number: AtomicU64,
    file: Mutex<File>,
}

impl WAL {
    pub fn new() -> WAL {
        let f = OpenOptions::new()
            .append(true)
                .create(true) // Optionally create the file if it doesn't already exist
                .open("/tmp/test_wal")
                .unwrap();
        WAL {
            registered_types: HashMap::new(),
            sequence_number: AtomicU64::new(1),
            file: Mutex::new(f),
        }
    }
}

impl Sequencer for WAL {
    fn take_next(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.fetch_add(1, Ordering::Relaxed) as u128))
    }

    fn poll_next(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.load(Ordering::Relaxed) as u128))
    }

    fn previous(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.load(Ordering::Relaxed) as u128 - 1))
    }
}

impl DurabilityService for WAL {
    fn register_record_type(&mut self, record_type: DurabilityRecordType, record_name: &'static str) {
        if self.registered_types.get(&record_type).map(|name| *name != record_name).unwrap_or(false) {
            panic!("Illegal state: two types of WAL records registered with same ID and different names.")
        }
        self.registered_types.insert(record_type, record_name);
    }

    fn sequenced_write(&self, record: &impl DurabilityRecord, record_name: &'static str) -> Result<SequenceNumber, DurabilityError> {
        debug_assert!(*self.registered_types.get(&record.record_type()).unwrap() == record_name);
        let mut file = self.file.lock().unwrap();

        record.serialise_into(file.deref_mut()).map(|_| self.take_next())
            .map_err(|err| DurabilityError {
                kind: DurabilityErrorKind::BincodeSerializeError {source: err }
            })
    }
}

