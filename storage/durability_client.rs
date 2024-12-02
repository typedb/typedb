/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    io::{self, Read, Write},
    sync::{mpsc, Arc},
};

use durability::{wal::WAL, DurabilityRecordType, DurabilityService, DurabilityServiceError, RawRecord};
use error::typedb_error;
use itertools::Itertools;

use crate::sequence_number::SequenceNumber;

pub trait DurabilityRecord: Sized {
    const RECORD_TYPE: DurabilityRecordType;
    const RECORD_NAME: &'static str;
    fn serialise_into(&self, writer: &mut impl Write) -> bincode::Result<()>;
    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self>;
}

pub trait SequencedDurabilityRecord: DurabilityRecord {}

pub trait UnsequencedDurabilityRecord: DurabilityRecord {}

/// A durability client must be able to submit records to the durability service, iterate through records from the service,
/// and in the future collect disjoint durability services' sequenced batches per-epoch and collate them into a single continuous sequence
///     Note: the challenge for this is managing the reverse, mapping a global ordering to durability services' ordering
pub trait DurabilityClient {
    fn register_record_type<Record: DurabilityRecord>(&mut self);

    fn current(&self) -> SequenceNumber;

    fn previous(&self) -> SequenceNumber;

    fn sequenced_write<Record>(&self, record: &Record) -> Result<SequenceNumber, DurabilityClientError>
    where
        Record: SequencedDurabilityRecord;

    fn unsequenced_write<Record>(&self, record: &Record) -> Result<(), DurabilityClientError>
    where
        Record: UnsequencedDurabilityRecord;

    fn request_sync(&self) -> mpsc::Receiver<()>;

    fn iter_from(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityClientError>>, DurabilityClientError>;

    fn iter_from_start(
        &self,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityClientError>>, DurabilityClientError> {
        self.iter_from(SequenceNumber::MIN)
    }

    fn iter_type_from<Record: DurabilityRecord>(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<(SequenceNumber, Record), DurabilityClientError>>, DurabilityClientError>;

    fn iter_sequenced_type_from<Record: SequencedDurabilityRecord>(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<(SequenceNumber, Record), DurabilityClientError>>, DurabilityClientError>
    {
        self.iter_type_from::<Record>(sequence_number)
    }

    fn iter_sequenced_type_from_start<Record: SequencedDurabilityRecord>(
        &self,
    ) -> Result<impl Iterator<Item = Result<(SequenceNumber, Record), DurabilityClientError>>, DurabilityClientError>
    {
        self.iter_sequenced_type_from::<Record>(SequenceNumber::MIN)
    }

    fn iter_unsequenced_type_from<Record: UnsequencedDurabilityRecord>(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<Record, DurabilityClientError>>, DurabilityClientError> {
        Ok(self.iter_type_from::<Record>(sequence_number)?.map_ok(|(_, record)| record))
    }

    fn iter_unsequenced_type_from_start<Record: UnsequencedDurabilityRecord>(
        &self,
    ) -> Result<impl Iterator<Item = Result<Record, DurabilityClientError>>, DurabilityClientError> {
        self.iter_unsequenced_type_from(SequenceNumber::MIN)
    }

    fn find_last_unsequenced_type<Record: UnsequencedDurabilityRecord>(
        &self,
    ) -> Result<Option<Record>, DurabilityClientError>;

    fn delete_durability(self) -> Result<(), DurabilityClientError>;

    fn reset(&mut self) -> Result<(), DurabilityClientError>;
}

#[derive(Debug)]
pub struct WALClient {
    wal: WAL,
}

impl WALClient {
    pub fn new(wal: WAL) -> Self {
        Self { wal }
    }

    fn serialise_record(record: &impl DurabilityRecord) -> Result<Vec<u8>, DurabilityClientError> {
        let mut buf = Vec::new();
        let mut encoder = lz4::EncoderBuilder::new()
            .build(&mut buf)
            .map_err(|err| DurabilityClientError::CompressionError { source: Arc::new(err) })?;
        record.serialise_into(&mut encoder)?;
        encoder.finish().1.map_err(|err| DurabilityClientError::CompressionError { source: Arc::new(err) })?;
        Ok(buf)
    }

    fn deserialise_record<Record: DurabilityRecord>(raw_bytes: &[u8]) -> Result<Record, DurabilityClientError> {
        let ptr = &mut &*raw_bytes;
        let mut decoder =
            lz4::Decoder::new(ptr).map_err(|err| DurabilityClientError::CompressionError { source: Arc::new(err) })?;
        let record = Record::deserialise_from(&mut decoder)
            .map_err(|err| DurabilityClientError::SerializeError { source: Arc::new(err) })?;
        Ok(record)
    }

    fn decompress(raw_bytes: &[u8]) -> Result<Vec<u8>, DurabilityClientError> {
        let mut buf = Vec::new();
        lz4::Decoder::new(&mut &*raw_bytes)
            .map_err(|err| DurabilityClientError::CompressionError { source: Arc::new(err) })?
            .read_to_end(&mut buf)
            .map_err(|err| DurabilityClientError::CompressionError { source: Arc::new(err) })?;
        Ok(buf)
    }
}

impl DurabilityClient for WALClient {
    fn request_sync(&self) -> mpsc::Receiver<()> {
        let WAIT_FOR_SYNC = true;
        self.wal.request_sync(WAIT_FOR_SYNC)
    }

    fn register_record_type<Record: DurabilityRecord>(&mut self) {
        self.wal.register_record_type(Record::RECORD_TYPE, Record::RECORD_NAME);
    }

    fn current(&self) -> SequenceNumber {
        self.wal.current()
    }

    fn previous(&self) -> SequenceNumber {
        self.wal.previous()
    }

    fn sequenced_write<Record>(&self, record: &Record) -> Result<SequenceNumber, DurabilityClientError>
    where
        Record: SequencedDurabilityRecord,
    {
        let serialised = Self::serialise_record(record)?;
        self.wal
            .sequenced_write(Record::RECORD_TYPE, &serialised)
            .map_err(|err| DurabilityClientError::ServiceError { source: err })
    }

    fn unsequenced_write<Record>(&self, record: &Record) -> Result<(), DurabilityClientError>
    where
        Record: UnsequencedDurabilityRecord,
    {
        let serialised = Self::serialise_record(record)?;
        self.wal
            .unsequenced_write(Record::RECORD_TYPE, &serialised)
            .map_err(|err| DurabilityClientError::ServiceError { source: err })
    }

    fn iter_from(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityClientError>>, DurabilityClientError> {
        self.wal.iter_any_from(sequence_number).map_err(|err| DurabilityClientError::ServiceError { source: err }).map(
            |iter| {
                iter.map(|item| {
                    item.map_err(|err| DurabilityClientError::ServiceError { source: err }).and_then(|raw_record| {
                        Ok(RawRecord {
                            record_type: raw_record.record_type,
                            sequence_number: raw_record.sequence_number,
                            bytes: Cow::Owned(Self::decompress(&raw_record.bytes)?),
                        })
                    })
                })
            },
        )
    }

    fn iter_type_from<Record>(
        &self,
        sequence_number: SequenceNumber,
    ) -> Result<impl Iterator<Item = Result<(SequenceNumber, Record), DurabilityClientError>>, DurabilityClientError>
    where
        Record: DurabilityRecord,
    {
        self.wal
            .iter_type_from(sequence_number, Record::RECORD_TYPE)
            .map_err(|err| DurabilityClientError::ServiceError { source: err })
            .map(|iter| {
                iter.map(|raw_item| match raw_item {
                    Ok(raw_record) => {
                        let record =
                            (raw_record.sequence_number, Self::deserialise_record::<Record>(&raw_record.bytes)?);
                        Ok(record)
                    }
                    Err(err) => Err(DurabilityClientError::ServiceError { source: err }),
                })
            })
    }

    fn find_last_unsequenced_type<Record: UnsequencedDurabilityRecord>(
        &self,
    ) -> Result<Option<Record>, DurabilityClientError> {
        match self.wal.find_last_type(Record::RECORD_TYPE) {
            Ok(Some(raw_record)) => Some(Self::deserialise_record::<Record>(&raw_record.bytes)).transpose(),
            Ok(None) => Ok(None),
            Err(err) => Err(DurabilityClientError::ServiceError { source: err }),
        }
    }

    fn delete_durability(self) -> Result<(), DurabilityClientError> {
        self.wal.delete_durability().map_err(|err| DurabilityClientError::ServiceError { source: err })
    }

    fn reset(&mut self) -> Result<(), DurabilityClientError> {
        self.wal.reset().map_err(|err| DurabilityClientError::ServiceError { source: err })
    }
}

typedb_error!(
    pub DurabilityClientError(component = "Durability client", prefix = "DUC") {
        SerializeError(1, "Durability client failed to serialise/deserialise durability record", ( source: Arc<bincode::Error> )),
        ServiceError(2, "Error from durability service.", ( source: DurabilityServiceError )),
        CompressionError(3, "Error while compressing durability record.", ( source: Arc<io::Error> )),
    }
);

impl From<bincode::Error> for DurabilityClientError {
    fn from(source: bincode::Error) -> Self {
        Self::SerializeError { source: Arc::new(source) }
    }
}
