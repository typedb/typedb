/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, io::Read, num::NonZeroU64};

use durability::DurabilityRecordType;
use logger::result::ResultExt;
use serde::{Deserialize, Serialize};

use crate::{
    durability_client::{DurabilityRecord, SequencedDurabilityRecord, UnsequencedDurabilityRecord},
    isolation_manager::{CommitDependency, DependentPut, IsolationConflict},
    snapshot::{buffer::OperationsBuffer, lock::LockType, write::Write},
    uniqueness::{SequenceNumber, TransactionId},
};

#[derive(Serialize, Deserialize)]
pub struct CommitRecord {
    // TODO: this could read-through to the WAL if we have to save memory?
    operations: OperationsBuffer,
    open_sequence_number: SequenceNumber,
    commit_type: CommitType,

    /// Optional transaction identifier used for efficient referencing to commit records in the WAL,
    /// avoiding excessive lookups through the whole filesystem.
    #[serde(default)]
    transaction_id: Option<TransactionId>,
}

#[derive(Serialize, Deserialize)]
struct LegacyCommitRecordV1 {
    operations: OperationsBuffer,
    open_sequence_number: SequenceNumber,
    commit_type: CommitType,
}

impl From<LegacyCommitRecordV1> for CommitRecord {
    fn from(legacy: LegacyCommitRecordV1) -> Self {
        CommitRecord::new(legacy.operations, legacy.open_sequence_number, legacy.commit_type)
    }
}

impl fmt::Debug for CommitRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitRecord")
            .field("open_sequence_number", &self.open_sequence_number)
            .field("transaction_id", &self.transaction_id)
            .field("commit_type", &self.commit_type)
            .field("operations", &self.operations)
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum CommitType {
    Data,
    Schema,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusRecord {
    pub(crate) commit_record_sequence_number: SequenceNumber,
    pub(crate) was_committed: bool,
}

impl CommitRecord {
    pub(crate) fn new(
        operations: OperationsBuffer,
        open_sequence_number: SequenceNumber,
        commit_type: CommitType,
    ) -> CommitRecord {
        let transaction_id = Some(TransactionId::new(open_sequence_number));
        CommitRecord { operations, open_sequence_number, commit_type, transaction_id }
    }

    pub fn operations(&self) -> &OperationsBuffer {
        &self.operations
    }

    pub fn into_operations(self) -> OperationsBuffer {
        self.operations
    }

    pub fn commit_type(&self) -> CommitType {
        self.commit_type
    }

    pub fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }

    fn deserialise_from(record_type: DurabilityRecordType, reader: impl Read)
    where
        Self: Sized,
    {
        assert_eq!(Self::RECORD_TYPE, record_type);
        // TODO: handle error with a better message
        bincode::deserialize_from(reader).unwrap_or_log()
    }

    pub(crate) fn compute_dependency(&self, predecessor: &CommitRecord) -> CommitDependency {
        // TODO: this can be optimised by some kind of bit-wise AND of two bloom filter-like data
        // structures first, since we assume few clashes this should mostly succeed
        // TODO: can be optimised with an intersection of two sorted iterators instead of iterate + gets

        let mut puts_to_update = Vec::new();

        // we check self operations against predecessor operations.
        //   if our buffer contains a delete, we check the predecessor doesn't have an Existing lock on it
        // We check

        let locks = self.operations().locks();
        let predecessor_locks = predecessor.operations().locks();
        for (write_buffer, pred_write_buffer) in self.operations().write_buffers().zip(predecessor.operations()) {
            let writes = write_buffer.writes();
            let predecessor_writes = pred_write_buffer.writes();

            for (key, write) in writes.iter() {
                if let Some(predecessor_write) = predecessor_writes.get(key) {
                    match (predecessor_write, write) {
                        (Write::Insert { .. } | Write::Put { .. }, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Inserted { reinsert: reinsert.clone() });
                        }
                        (Write::Delete, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Deleted { reinsert: reinsert.clone() });
                        }
                        _ => (),
                    }
                }
                if matches!(write, Write::Delete) && matches!(predecessor_locks.get(key), Some(LockType::Unmodifiable))
                {
                    return CommitDependency::Conflict(IsolationConflict::DeletingRequiredKey);
                }
            }

            // TODO: this is ineffecient since we loop over all locks each time - should we locks into keyspaces?
            //    Investigate
            for (key, lock) in locks.iter() {
                if matches!(lock, LockType::Unmodifiable) {
                    if let Some(Write::Delete) = predecessor_writes.get(key) {
                        return CommitDependency::Conflict(IsolationConflict::RequireDeletedKey);
                    }
                }
            }
        }

        for (key, lock) in locks.iter() {
            if matches!(lock, LockType::Exclusive) && matches!(predecessor_locks.get(key), Some(LockType::Exclusive)) {
                return CommitDependency::Conflict(IsolationConflict::ExclusiveLock);
            }
        }

        if puts_to_update.is_empty() {
            CommitDependency::Independent
        } else {
            CommitDependency::DependentPuts { puts: puts_to_update }
        }
    }
}

impl DurabilityRecord for CommitRecord {
    const RECORD_TYPE: DurabilityRecordType = 0;

    const RECORD_NAME: &'static str = "commit_record";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        debug_assert_eq!(
            bincode::serialize(
                &bincode::deserialize::<CommitRecord>(bincode::serialize(&self).as_ref().unwrap()).unwrap()
            )
            .unwrap(),
            bincode::serialize(self).unwrap()
        );
        bincode::serialize_into(writer, &self)
    }

    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self> {
        // https://github.com/bincode-org/bincode/issues/633
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).map_err(|e| bincode::ErrorKind::Io(e))?;
        match bincode::deserialize::<CommitRecord>(&buf) {
            Ok(record) => Ok(record),
            Err(_error) => {
                // fallback to legacy
                bincode::deserialize::<LegacyCommitRecordV1>(&buf).map(|legacy| CommitRecord::from(legacy))
            }
        }
    }
}

impl SequencedDurabilityRecord for CommitRecord {}

impl StatusRecord {
    pub(crate) fn new(sequence_number: SequenceNumber, committed: bool) -> StatusRecord {
        StatusRecord { commit_record_sequence_number: sequence_number, was_committed: committed }
    }

    pub(crate) fn was_committed(&self) -> bool {
        self.was_committed
    }

    pub(crate) fn commit_record_sequence_number(&self) -> SequenceNumber {
        self.commit_record_sequence_number
    }
}

impl DurabilityRecord for StatusRecord {
    const RECORD_TYPE: DurabilityRecordType = 1;
    const RECORD_NAME: &'static str = "status_record";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        debug_assert_eq!(
            bincode::serialize(
                &bincode::deserialize::<StatusRecord>(bincode::serialize(&self).as_ref().unwrap()).unwrap()
            )
            .unwrap(),
            bincode::serialize(self).unwrap()
        );
        bincode::serialize_into(writer, &self)
    }

    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self> {
        // https://github.com/bincode-org/bincode/issues/633
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        bincode::deserialize(&buf)
    }
}

impl UnsequencedDurabilityRecord for StatusRecord {}
