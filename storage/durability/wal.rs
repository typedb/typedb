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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{DurabilityRecord, DurabilityRecordType, DurabilityService, SequenceNumber, Sequencer};

pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, &'static str>,
    sequence_number: AtomicU64,
}

impl WAL {
    pub fn new() -> WAL {
        WAL {
            registered_types: HashMap::new(),
            sequence_number: AtomicU64::new(1),
        }
    }
}

impl Sequencer for WAL {
    fn take_next(&self) -> SequenceNumber {
        SequenceNumber::new(self.sequence_number.fetch_add(1, Ordering::Relaxed))
    }

    fn poll_next(&self) -> SequenceNumber {
        SequenceNumber::new(self.sequence_number.load(Ordering::Relaxed))
    }

    fn previous(&self) -> SequenceNumber {
        SequenceNumber::new(self.sequence_number.load(Ordering::Relaxed) - 1)
    }
}

impl DurabilityService for WAL {
    fn register_record_type(&mut self, record_type: DurabilityRecordType, record_name: &'static str) {
        if self.registered_types.get(&record_type).map(|name| *name != record_name).unwrap_or(false) {
            panic!("Illegal state: two types of WAL records registered with same ID and different names.")
        }
        self.registered_types.insert(record_type, record_name);
    }

    fn sequenced_write(&self, record: impl DurabilityRecord, record_name: &'static str) -> SequenceNumber {
        debug_assert!(*self.registered_types.get(&record.record_type()).unwrap() == record_name);
        // TODO: serialise into file and wait for fsync
        self.take_next()
    }

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, &dyn DurabilityRecord)>> {
        todo!()
    }
}

