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

use std::{
    error::Error,
    fmt,
    io::{self, Read, Write},
};

use primitive::u80::U80;
use serde::{Deserialize, Serialize};

pub mod wal;

pub type Result<T> = std::result::Result<T, DurabilityError>;

#[derive(Debug)]
struct RecordHeader {
    sequence_number: SequenceNumber,
    len: u32,
    record_type: DurabilityRecordType,
}

#[derive(Debug)]
pub struct RawRecord {
    pub sequence_number: SequenceNumber,
    pub record_type: DurabilityRecordType,
    pub bytes: Box<[u8]>,
}

pub trait DurabilityService: Sequencer {
    fn register_record_type<Record: DurabilityRecord>(&mut self);

    fn sequenced_write<Record>(&self, record: &Record) -> Result<SequenceNumber>
    where
        Record: DurabilityRecord;

    fn iter_from(&self, sequence_number: SequenceNumber) -> io::Result<impl Iterator<Item = io::Result<RawRecord>>>;

    fn iter_type_from<Record: DurabilityRecord>(
        &self,
        sequence_number: SequenceNumber,
    ) -> io::Result<impl Iterator<Item = Result<(SequenceNumber, Record)>>> {
        Ok(self.iter_from(sequence_number)?.map(|res| {
            let raw = res?;
            Ok((raw.sequence_number, Record::deserialize_from(&mut &*raw.bytes)?))
        }))
    }

    fn checkpoint(&self) -> Result<()>;
    fn recover(&self) -> io::Result<impl Iterator<Item = io::Result<RawRecord>>>;
}

pub type DurabilityRecordType = u8;

pub trait DurabilityRecord: Sized {
    const RECORD_TYPE: DurabilityRecordType;
    const RECORD_NAME: &'static str;
    fn serialise_into(&self, writer: &mut impl Write) -> bincode::Result<()>;
    fn deserialize_from(writer: &mut impl Read) -> bincode::Result<Self>;
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SequenceNumber {
    number: U80,
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeqNr[{}]", self.number.number())
    }
}

impl SequenceNumber {
    pub const MAX: Self = Self { number: U80::MAX };

    pub fn new(number: U80) -> Self {
        Self { number }
    }

    pub fn next(&self) -> Self {
        Self { number: self.number + U80::new(1) }
    }

    pub fn previous(&self) -> Self {
        Self { number: self.number - U80::new(1) }
    }

    pub fn number(&self) -> U80 {
        self.number
    }

    pub fn serialise_be_into(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), U80::BYTES);
        let number_bytes = self.number.to_be_bytes();
        bytes.copy_from_slice(&number_bytes)
    }

    pub fn to_be_bytes(&self) -> [u8; U80::BYTES] {
        self.number.to_be_bytes()
    }

    pub fn invert(&self) -> Self {
        Self { number: U80::MAX - self.number }
    }

    pub const fn serialised_len() -> usize {
        U80::BYTES
    }
}

impl From<u128> for SequenceNumber {
    fn from(value: u128) -> Self {
        Self::new(U80::new(value))
    }
}

pub trait Sequencer {
    fn increment(&self) -> SequenceNumber;

    fn current(&self) -> SequenceNumber;

    fn previous(&self) -> SequenceNumber;
}

#[derive(Debug)]
pub struct DurabilityError {
    pub kind: DurabilityErrorKind,
}

#[derive(Debug)]
pub enum DurabilityErrorKind {
    #[non_exhaustive]
    BincodeSerializeError { source: bincode::Error },

    #[non_exhaustive]
    IOError { source: io::Error },
}

impl fmt::Display for DurabilityError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl From<bincode::Error> for DurabilityError {
    fn from(source: bincode::Error) -> Self {
        Self { kind: DurabilityErrorKind::BincodeSerializeError { source } }
    }
}

impl From<io::Error> for DurabilityError {
    fn from(source: io::Error) -> Self {
        Self { kind: DurabilityErrorKind::IOError { source } }
    }
}

impl Error for DurabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            DurabilityErrorKind::BincodeSerializeError { source, .. } => Some(source),
            DurabilityErrorKind::IOError { source, .. } => Some(source),
        }
    }
}
