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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::{Write};
use std::ops::{Add, Sub};
use primitive::{U80};

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

    fn sequenced_write(&self, record: &impl DurabilityRecord, record_name: &'static str) -> Result<SequenceNumber, DurabilityError>;

    // fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, DurabilityRecordType, dyn Read)>>;
}

pub type DurabilityRecordType = u8;

pub trait DurabilityRecord {
    fn record_type(&self) -> DurabilityRecordType;

    fn serialise_into(&self, writer: &mut impl Write) -> bincode::Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SequenceNumber {
    number: U80,
}

impl Display for SequenceNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqNr[{}]", self.number.number())
    }
}

impl SequenceNumber {
    pub const MAX: SequenceNumber = SequenceNumber { number: U80::MAX };

    pub fn new(number: U80) -> SequenceNumber {
        SequenceNumber { number: number }
    }

    pub fn plus(&self, number: U80) -> SequenceNumber {
        return SequenceNumber { number: self.number + number };
    }

    pub fn number(&self) -> U80 {
        self.number
    }

    pub fn serialise_be_into(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), U80::BYTES);
        let number_bytes = self.number.to_be_bytes();
        bytes.copy_from_slice(&number_bytes)
    }

    pub fn serialise_be(&self) -> [u8; U80::BYTES] {
        self.number.to_be_bytes()
    }

    pub fn invert(&self) -> SequenceNumber {
        SequenceNumber::MAX - *self
    }

    pub const fn serialised_len() -> usize {
        U80::BYTES
    }
}

impl Add for SequenceNumber {
    type Output = SequenceNumber;

    fn add(self, rhs: Self) -> Self::Output {
        SequenceNumber { number: self.number + rhs.number }
    }
}

impl Sub for SequenceNumber {
    type Output = SequenceNumber;

    fn sub(self, rhs: Self) -> Self::Output {
        SequenceNumber { number: self.number - rhs.number }
    }
}

pub trait Sequencer {
    fn take_next(&self) -> SequenceNumber;

    fn poll_next(&self) -> SequenceNumber;

    fn previous(&self) -> SequenceNumber;
}


#[derive(Debug)]
pub struct DurabilityError {
    pub kind: DurabilityErrorKind,
}

#[derive(Debug)]
pub enum DurabilityErrorKind {
    BincodeSerializeError { source: bincode::Error },
}

impl Display for DurabilityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for DurabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            DurabilityErrorKind::BincodeSerializeError { source, .. } => Some(source),
        }
    }
}