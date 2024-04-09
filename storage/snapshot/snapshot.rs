/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};
use std::io::Read;
use std::iter::FlatMap;
use std::slice::Iter;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use durability::{DurabilityService, SequenceNumber};
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};

use crate::{
    error::MVCCStorageError,
    isolation_manager::CommitRecord,
    iterator::MVCCReadError,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    MVCCStorage,
};
use crate::snapshot::buffer::KeyspaceBuffer;
use crate::snapshot::write::Write;

use super::{buffer::KeyspaceBuffers, iterator::SnapshotRangeIterator};

#[derive(Debug)]
pub enum Snapshot<'storage, D> {
    Read(ReadSnapshot<'storage, D>),
    Write(WriteSnapshot<'storage, D>),
}

impl<'storage, D> Snapshot<'storage, D> {
    pub fn get<const KS: usize>(
        &self,
        key: StorageKeyReference<'_>,
    ) -> Result<Option<ByteArray<KS>>, SnapshotGetError> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get(key),
            Snapshot::Write(snapshot) => snapshot.get(key),
        }
    }

    pub fn get_mapped<T>(
        &self,
        key: StorageKeyReference<'_>,
        mapper: impl FnMut(ByteReference<'_>) -> T,
    ) -> Result<Option<T>, SnapshotGetError> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get_mapped(key, mapper),
            Snapshot::Write(snapshot) => snapshot.get_mapped(key, mapper),
        }
    }

    pub fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: PrefixRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator<'this, PS> {
        match self {
            Snapshot::Read(snapshot) => snapshot.iterate_range(range),
            Snapshot::Write(snapshot) => snapshot.iterate_range(range),
        }
    }

    pub fn close(self) {
        match self {
            Snapshot::Read(snapshot) => snapshot.close_resources(),
            Snapshot::Write(snapshot) => snapshot.close_resources(),
        }
    }

    pub fn open_sequence_number(&self) -> SequenceNumber {
        match self {
            Snapshot::Read(snapshot) => snapshot.open_sequence_number,
            Snapshot::Write(snapshot) => snapshot.open_sequence_number,
        }
    }
}

pub trait ReadableSnapshot {

    fn open_sequence_number(&self) -> SequenceNumber;

    fn get<const KS: usize>(&self, key: StorageKeyReference<'_>) -> Result<Option<ByteArray<KS>>, SnapshotGetError>;

    fn get_mapped<T>(&self, key: StorageKeyReference<'_>,
                     mapper: impl FnMut(ByteReference<'_>) -> T,
    ) -> Result<Option<T>, SnapshotGetError>;

    fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: PrefixRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator<'this, PS>;
}

pub trait WritableSnapshot: ReadableSnapshot {
    /// Insert a key with a new version
    fn insert(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>);

    fn insert_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>);

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    fn put(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>);

    fn put_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>);

    /// Insert a delete marker for the key with a new version
    fn delete(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>);

    /// Get a Value, and mark it as a required key
    fn get_required(
        &self,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
    ) -> Result<ByteArray<BUFFER_VALUE_INLINE>, SnapshotGetError>;

    fn record_lock(&self, key: StorageKey<'static, BUFFER_KEY_INLINE>);

    fn close_resources(&self);
}

pub trait CommittableSnapshot<D> where D: DurabilityService {
    fn commit(self) -> Result<(), SnapshotError>;

    fn into_commit_record(self) -> CommitRecord;
}

#[derive(Debug)]
pub struct ReadSnapshot<'storage, D> {
    storage: &'storage MVCCStorage<D>,
    open_sequence_number: SequenceNumber,
}

impl<'storage, D> ReadSnapshot<'storage, D> {
    pub(crate) fn new(storage: &'storage MVCCStorage<D>, open_sequence_number: SequenceNumber) -> Self {
        // Note: for serialisability, we would need to register the open transaction to the IsolationManager
        ReadSnapshot { storage, open_sequence_number }
    }

    pub fn close_resources(self) {}
}

impl<'storage, D> ReadableSnapshot for ReadSnapshot<'storage, D> {
    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn get<const KS: usize>(
        &self,
        key: StorageKeyReference<'_>,
    ) -> Result<Option<ByteArray<KS>>, SnapshotGetError> {
        // TODO: this clone may not be necessary - we could pass a reference up?
        self.storage
            .get(key, self.open_sequence_number, |reference| ByteArray::from(reference))
            .map_err(|error| SnapshotGetError::MVCCRead { source: error })
    }

    fn get_mapped<T>(
        &self,
        key: StorageKeyReference<'_>,
        mapper: impl FnMut(ByteReference<'_>) -> T,
    ) -> Result<Option<T>, SnapshotGetError> {
        self.storage
            .get(key, self.open_sequence_number, mapper)
            .map_err(|error| SnapshotGetError::MVCCRead { source: error })
    }

    fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: PrefixRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator<'this, PS> {
        let mvcc_iterator = self.storage.iterate_range(range, self.open_sequence_number);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }
}

#[derive(Debug)]
pub struct WriteSnapshot<'storage, D> {
    storage: &'storage MVCCStorage<D>,
    buffers: KeyspaceBuffers,
    open_sequence_number: SequenceNumber,
}

impl<'storage, D> WriteSnapshot<'storage, D> {
    pub(crate) fn new(storage: &'storage MVCCStorage<D>, open_sequence_number: SequenceNumber) -> Self {
        storage.isolation_manager.opened_for_read(open_sequence_number);
        WriteSnapshot { storage, buffers: KeyspaceBuffers::new(), open_sequence_number }
    }

    pub fn iterate_writes(&self) -> impl Iterator<Item=(StorageKeyArray<64>, Write)> + '_ {
        self.buffers.iter().flat_map(|buffer| {
            // note: this currently copies all the buffers
            buffer.iterate_range(PrefixRange::new_unbounded(Bytes::Array(ByteArray::<BUFFER_KEY_INLINE>::empty())))
                .into_range().into_iter()
        })
    }
}

impl<'storage, D> ReadableSnapshot for WriteSnapshot<'storage, D> {

    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        match self.buffers.get(key.keyspace_id()).get(key.bytes()) {
            None => self
                .storage
                .get(key, self.open_sequence_number, |reference| ByteArray::from(reference))
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
            some => Ok(some),
        }
    }

    fn get_mapped<T>(
        &self,
        key: StorageKeyReference<'_>,
        mut mapper: impl FnMut(ByteReference<'_>) -> T,
    ) -> Result<Option<T>, SnapshotGetError> {
        match self.buffers.get(key.keyspace_id()).get::<BUFFER_VALUE_INLINE>(key.bytes()) {
            None => self
                .storage
                .get(key, self.open_sequence_number, mapper)
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
            Some(value) => Ok(Some(mapper(ByteReference::from(&value)))),
        }
    }

    fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: PrefixRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator<'this, PS> {
        let buffered_iterator = self
            .buffers
            .get(range.start().keyspace_id())
            .iterate_range(range.clone().map(|k| k.into_byte_array_or_ref()));
        let storage_iterator = self.storage.iterate_range(range, self.open_sequence_number);
        SnapshotRangeIterator::new(storage_iterator, Some(buffered_iterator))
    }
}

impl<'storage, D> WritableSnapshot for WriteSnapshot<'storage, D> {
    /// Insert a key with a new version
    fn insert(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.insert_val(key, ByteArray::empty())
    }

    fn insert_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.buffers.get(keyspace_id).insert(byte_array, value);
    }

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    fn put(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.put_val(key, ByteArray::empty())
    }

    fn put_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.buffers.get(keyspace_id).put(byte_array, value);
    }

    /// Insert a delete marker for the key with a new version
    fn delete(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.buffers.get(keyspace_id).delete(byte_array);
    }

    /// Get a Value, and mark it as a required key
    fn get_required(
        &self,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
    ) -> Result<ByteArray<BUFFER_VALUE_INLINE>, SnapshotGetError> {
        let keyspace_id = key.keyspace_id();
        let buffer = self.buffers.get(keyspace_id);
        let existing = buffer.get(key.bytes());
        if let Some(existing) = existing {
            Ok(existing)
        } else {
            let storage_value = self
                .storage
                .get(key.as_reference(), self.open_sequence_number, |reference| ByteArray::from(reference))
                .map_err(|error| SnapshotGetError::MVCCRead { source: error })?;
            if let Some(value) = storage_value {
                buffer.require_exists(ByteArray::copy(key.bytes()), value.clone());
                Ok(value)
            } else {
                // TODO: what if the user concurrent requires a concept while deleting it in another query
                unreachable!("Require key exists in snapshot or in storage.");
            }
        }
    }

    fn record_lock(&self, key: StorageKey<'static, BUFFER_KEY_INLINE>) {
        todo!()
    }

    fn close_resources(&self) {
        self.storage.closed_snapshot_write(self.open_sequence_number());
    }
}

impl<'txn, D: DurabilityService> CommittableSnapshot<D> for WriteSnapshot<'txn, D> {

    // TODO: extract these two methods into separate trait
    fn commit(self) -> Result<(), SnapshotError> {
        if self.buffers.is_empty() {
            Ok(())
        } else {
            self.storage.snapshot_commit(self).map_err(|err| SnapshotError::Commit { source: err })
        }
    }

    fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(self.buffers, self.open_sequence_number)
    }

}

//  TODO: in the current version, we should need to close resouces on drop because we need to notify Isolation the txn is closed.
//        However in the next iteration of IsolationManager, we don't need to record readers at all.
//
// impl Drop for WriteSnapshot<'_> {
//     fn drop(&mut self) {
//         self.close_resources();
//     }
// }

#[derive(Debug)]
pub enum SnapshotError {
    Commit { source: MVCCStorageError },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::Commit { source, .. } => {
                write!(f, "SnapshotError::Commit caused by: {}", source)
            }
        }
    }
}

impl Error for SnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Commit { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum SnapshotGetError {
    MVCCRead { source: MVCCReadError },
}

impl fmt::Display for SnapshotGetError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SnapshotGetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MVCCRead { source, .. } => Some(source),
        }
    }
}
