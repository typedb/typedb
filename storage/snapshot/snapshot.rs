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

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use durability::{DurabilityService, SequenceNumber};
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};

use crate::{
    isolation_manager::CommitRecord,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::{
        buffer::KeyspaceBuffers,
        error::{SnapshotError, SnapshotErrorKind},
        iterator::SnapshotRangeIterator,
    },
    MVCCStorage,
};

#[derive(Debug)]
pub enum Snapshot<'storage, D> {
    Read(ReadSnapshot<'storage, D>),
    Write(WriteSnapshot<'storage, D>),
}

impl<'storage, D> Snapshot<'storage, D> {
    pub fn get<const KS: usize>(&self, key: StorageKeyReference<'_>) -> Option<ByteArray<KS>> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get(key),
            Snapshot::Write(snapshot) => snapshot.get(key),
        }
    }

    pub fn get_mapped<T>(&self, key: StorageKeyReference<'_>, mapper: impl FnMut(ByteReference<'_>) -> T) -> Option<T> {
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
}

impl<'storage, D> ReadSnapshot<'storage, D> {
    pub fn get<const KS: usize>(&self, key: StorageKeyReference<'_>) -> Option<ByteArray<KS>> {
        // TODO: this clone may not be necessary - we could pass a reference up?
        self.storage.get(key, self.open_sequence_number, |reference| ByteArray::from(reference))
    }

    pub fn get_mapped<T>(&self, key: StorageKeyReference<'_>, mapper: impl FnMut(ByteReference<'_>) -> T) -> Option<T> {
        self.storage.get(key, self.open_sequence_number, mapper)
    }

    pub fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: PrefixRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator<'this, PS> {
        let mvcc_iterator = self.storage.iterate_range(range, self.open_sequence_number);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }

    pub fn close_resources(self) {}
}

#[derive(Debug)]
pub struct WriteSnapshot<'storage, D> {
    storage: &'storage MVCCStorage<D>,
    buffers: KeyspaceBuffers,
    open_sequence_number: SequenceNumber,
}

impl<'storage, D> WriteSnapshot<'storage, D> {
    pub(crate) fn new(storage: &'storage MVCCStorage<D>, open_sequence_number: SequenceNumber) -> Self {
        storage.isolation_manager.opened(&open_sequence_number);
        WriteSnapshot { storage, buffers: KeyspaceBuffers::new(), open_sequence_number }
    }
}

impl<'storage, D> WriteSnapshot<'storage, D> {
    /// Insert a key with a new version
    pub fn insert(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.insert_val(key, ByteArray::empty())
    }

    pub fn insert_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.buffers.get(keyspace_id).insert(byte_array, value);
    }

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    pub fn put(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.put_val(key, ByteArray::empty())
    }

    pub fn put_val(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let buffer = self.buffers.get(keyspace_id);
        let existing_buffered = buffer.contains(key.byte_array());
        if !existing_buffered {
            let wrapped = StorageKeyReference::from(&key);
            let existing_stored = self.storage.get(wrapped, self.open_sequence_number, |reference| {
                // Only copy if the value is the same
                if reference.bytes() == value.bytes() {
                    Some(ByteArray::from(reference))
                } else {
                    None
                }
            });
            let byte_array = key.into_byte_array();
            if let Some(Some(existing_stored)) = existing_stored {
                buffer.insert_preexisting(byte_array, existing_stored);
            } else {
                buffer.insert(byte_array, value)
            }
        } else {
            // TODO: replace existing buffered write. If it contains a preexisting, we can continue to use it
            todo!()
        }
    }

    /// Insert a delete marker for the key with a new version
    pub fn delete(&self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.buffers.get(keyspace_id).delete(byte_array);
    }

    /// Get a Value, and mark it as a required key
    pub fn get_required(&self, key: StorageKey<'_, BUFFER_KEY_INLINE>) -> ByteArray<BUFFER_VALUE_INLINE> {
        let keyspace_id = key.keyspace_id();
        let buffer = self.buffers.get(keyspace_id);
        let existing = buffer.get(key.bytes());
        if let Some(existing) = existing {
            existing
        } else {
            let storage_value =
                self.storage.get(key.as_reference(), self.open_sequence_number, |reference| ByteArray::from(reference));
            if let Some(value) = storage_value {
                buffer.require_exists(ByteArray::copy(key.bytes()), value.clone());
                value
            } else {
                // TODO: what if the user concurrent requires a concept while deleting it in another query
                unreachable!("Require key exists in snapshot or in storage.");
            }
        }
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    pub fn get<const INLINE_BYTES: usize>(&self, key: StorageKeyReference<'_>) -> Option<ByteArray<INLINE_BYTES>> {
        let keyspace_id = key.keyspace_id();
        let existing_value = self.buffers.get(keyspace_id).get(key.bytes());
        existing_value.map_or_else(
            || self.storage.get(key, self.open_sequence_number, |reference| ByteArray::from(reference)),
            Some,
        )
    }

    pub fn get_mapped<T>(
        &self,
        key: StorageKeyReference<'_>,
        mut mapper: impl FnMut(ByteReference<'_>) -> T,
    ) -> Option<T> {
        let keyspace_id = key.keyspace_id();
        let existing_value = self.buffers.get(keyspace_id).get(key.bytes());
        existing_value
            .map(|value: ByteArray<BUFFER_VALUE_INLINE>| mapper(ByteReference::from(&value)))
            .or_else(|| self.storage.get(key, self.open_sequence_number, mapper))
    }

    pub fn iterate_range<'this, const PS: usize>(
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

    pub fn commit(self) -> Result<(), SnapshotError>
    where
        D: DurabilityService,
    {
        if self.buffers.is_empty() {
            Ok(())
        } else {
            self.storage
                .snapshot_commit(self)
                .map_err(|err| SnapshotError { kind: SnapshotErrorKind::FailedCommit { source: err } })
        }
    }

    pub(crate) fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(self.buffers, self.open_sequence_number)
    }

    pub(crate) fn open_sequence_number(&self) -> &SequenceNumber {
        &self.open_sequence_number
    }

    pub fn close_resources(&self) {
        self.storage.closed_snapshot_write(self.open_sequence_number());
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
