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

use std::iter::empty;

pub struct WAL {}

///
/// Questions:
///     1. Single file write vs multi-file write with multiple threads - performance implication?
///     2. Recovery/Checksum requirements - what are the failure modes
///     3. How to benchmark
///
impl WAL {
    fn sequenced_write(&self, record: impl Record) -> SequenceNumber {
        todo!()
    }

    fn last_sequence_number(&self) -> SequenceNumber {
        todo!()
    }

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> impl Iterator<Item=(SequenceNumber, &dyn Record)> {
        empty()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SequenceNumber {
    pub number: u64,
}

pub trait Record {
}
