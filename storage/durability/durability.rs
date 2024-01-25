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

pub mod wal;

pub trait DurabilityService: Sequencer {
    fn sequenced_write(&self, record: impl Record) -> SequenceNumber;

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, &dyn Record)>>;
}

pub trait Record {
    fn as_bytes(&self) -> &[u8];
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SequenceNumber {
    number: u64,
}

impl SequenceNumber {

    pub fn new(number: u64) -> SequenceNumber {
        SequenceNumber { number: number }
    }

    pub fn plus(&self, number: u64) -> SequenceNumber {
        return SequenceNumber { number: self.number + number }
    }

    pub fn number(&self) -> u64 {
        self.number
    }
}

pub trait Sequencer {

    fn take_next(&self) -> SequenceNumber;

    fn poll_next(&self) -> SequenceNumber;

    fn previous(&self) -> SequenceNumber;
}
