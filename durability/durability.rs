/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use std::{
    borrow::Cow,
    error::Error,
    fmt, io,
    ops::{Add, AddAssign, Sub},
};

use serde::{Deserialize, Serialize};

use crate::wal::WALError;

pub mod wal;

pub trait DurabilityService {
    fn register_record_type(&mut self, record_type: DurabilityRecordType, record_name: &str);

    fn sequenced_write(
        &self,
        record_type: DurabilityRecordType,
        bytes: &[u8],
    ) -> Result<DurabilitySequenceNumber, DurabilityServiceError>;

    fn unsequenced_write(&self, record_type: DurabilityRecordType, bytes: &[u8]) -> Result<(), DurabilityServiceError>;

    fn iter_any_from(
        &self,
        sequence_number: DurabilitySequenceNumber,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityServiceError>>, DurabilityServiceError>;

    fn iter_type_from(
        &self,
        sequence_number: DurabilitySequenceNumber,
        record_type: DurabilityRecordType,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityServiceError>>, DurabilityServiceError>;

    fn find_last_type(
        &self,
        record_type: DurabilityRecordType,
    ) -> Result<Option<RawRecord<'static>>, DurabilityServiceError>;

    fn delete_durability(self) -> Result<(), DurabilityServiceError>;
}

pub type DurabilityRecordType = u8;

#[derive(Debug)]
pub struct RawRecord<'a> {
    pub sequence_number: DurabilitySequenceNumber,
    pub record_type: DurabilityRecordType,
    pub bytes: Cow<'a, [u8]>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DurabilitySequenceNumber {
    number: u64,
}

impl fmt::Display for DurabilitySequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeqNr[{}]", self.number)
    }
}

impl DurabilitySequenceNumber {
    pub const MIN: Self = Self { number: u64::MIN };
    pub const MAX: Self = Self { number: u64::MAX };

    pub fn new(number: u64) -> Self {
        Self { number }
    }

    pub fn next(&self) -> Self {
        Self { number: self.number + 1 }
    }

    pub fn previous(&self) -> Self {
        Self { number: self.number - 1 }
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn serialise_be_into(&self, bytes: &mut [u8]) {
        assert_eq!(bytes.len(), std::mem::size_of::<u64>());
        let number_bytes = self.number.to_be_bytes();
        bytes.copy_from_slice(&number_bytes)
    }

    pub fn from_be_bytes(bytes: &[u8]) -> Self {
        let mut u64_bytes = [0; 8];
        u64_bytes.copy_from_slice(bytes);
        Self::from(u64::from_be_bytes(u64_bytes))
    }

    pub fn to_be_bytes(&self) -> [u8; std::mem::size_of::<u64>()] {
        self.number.to_be_bytes()
    }

    pub fn invert(&self) -> Self {
        Self { number: u64::MAX - self.number }
    }

    pub const fn serialised_len() -> usize {
        std::mem::size_of::<u64>()
    }
}

impl From<u64> for DurabilitySequenceNumber {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Add<usize> for DurabilitySequenceNumber {
    type Output = DurabilitySequenceNumber;

    fn add(self, rhs: usize) -> Self::Output {
        DurabilitySequenceNumber::from(self.number + rhs as u64)
    }
}

impl Sub<usize> for DurabilitySequenceNumber {
    type Output = DurabilitySequenceNumber;

    fn sub(self, rhs: usize) -> Self::Output {
        DurabilitySequenceNumber::from(self.number - rhs as u64)
    }
}

impl AddAssign<usize> for DurabilitySequenceNumber {
    fn add_assign(&mut self, rhs: usize) {
        self.number = self.number + rhs as u64
    }
}

impl Sub<DurabilitySequenceNumber> for DurabilitySequenceNumber {
    type Output = usize;

    fn sub(self, rhs: DurabilitySequenceNumber) -> Self::Output {
        (self.number - rhs.number) as usize
    }
}

#[derive(Debug)]
pub enum DurabilityServiceError {
    // #[non_exhaustive]
    // BincodeSerialize { source: bincode::Error },
    #[non_exhaustive]
    IO {
        source: io::Error,
    },
    WAL {
        source: WALError,
    },

    DeleteFailed {
        source: io::Error,
    },
}

impl fmt::Display for DurabilityServiceError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
//
// impl From<bincode::Error> for DurabilityError {
//     fn from(source: bincode::Error) -> Self {
//         Self::BincodeSerialize { source }
//     }
// }

impl From<io::Error> for DurabilityServiceError {
    fn from(source: io::Error) -> Self {
        Self::IO { source }
    }
}

impl Error for DurabilityServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            // Self::BincodeSerialize { source, .. } => Some(source),
            Self::IO { source, .. } => Some(source),
            Self::WAL { source, .. } => Some(source),
            Self::DeleteFailed { source, .. } => Some(source),
        }
    }
}
