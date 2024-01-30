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

use serde::{Deserialize, Serialize};

pub mod wal;

///
/// Notes:
///     We can provide to a DurabilityService for Records
///     Records have a serialised Byte representations and type (ID)
///
///     We have two choices:
///         1. Long files (~ 100s MB) with many records embedded into fixed-sized blocks (fixed size = seekable)
///         2. Shorter files (~ 10s MB) without fixed sized blocks internally.
///
///     We should aim to minimise the amount of data ending on the Disk, which means compression is critical.
///     For this, having rolling shorter files that are compressed in larger, irregular chunks as they are written,
///     is more optimal. Having rolling shorter files named by the first sequence number they contain,
///     still allows skipping through the logical WAL by binary searching file names,
///     then doing a scan through 10s of MB of data. This is acceptable if:
///         1. we rarely need to do point look ups in data (iterating is amortises the cost)
///         2. the costs of reading 10s of extra MB during a lookup is acceptable to overall performance.
///
///     Either solution will allow writing SerialisedRecord (data + type tag), and retrieving SerialisedRecords.
///
///     The final piece of the puzzle will be: how do we ensure with very high likelihood that we don't have two
///     different implementations of SerialisedRecord using the same type tag?
///
///     First idea:
///         On creation, we register a "name" and "type ID" against the DurabilityService.
///         The service only serialises Records of a TypeID + Name that have been recognised - this can fail with a debug_assert
///         The service should check that all TypeIDs registered have a different Name.
///
/// Other questions:
///     1. Recovery/Checksum requirements - what are the failure modes
///     2. How to benchmark

pub trait DurabilityService: Sequencer {
    fn register_record_type(&mut self, record_type: DurabilityRecordType, record_name: &'static str);

    fn sequenced_write(&self, record: impl DurabilityRecord, record_name: &'static str) -> SequenceNumber;

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, &dyn DurabilityRecord)>>;
}

pub type DurabilityRecordType = u8;

pub trait DurabilityRecord {
    fn record_type(&self) -> DurabilityRecordType;

    fn bytes(&self) -> &[u8];
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
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
