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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{DurabilityService, Record, SequenceNumber, Sequencer};

pub struct WAL {
    sequence_number: AtomicU64,
}

impl WAL {
    pub fn new() -> WAL {
        WAL {
            sequence_number: AtomicU64::new(0),
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

///
/// Questions:
///     1. Single file write vs multi-file write with multiple threads - performance implication?
///     2. Recovery/Checksum requirements - what are the failure modes
///     3. How to benchmark
///
impl DurabilityService for WAL {
    fn sequenced_write(&self, record: impl Record) -> SequenceNumber {
        self.take_next()
    }

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, &dyn Record)>> {
        todo!()
    }
}

