/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{any::type_name, error::Error, fmt, iter::empty, sync::Arc};

use bytes::byte_array::ByteArray;
use error::typedb_error;
use lending_iterator::LendingIterator;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::{CommitProfile, StorageCounters},
};

use crate::{
    durability_client::DurabilityClient,
    isolation_manager::{CommitRecord, CommitType},
    iterator::MVCCReadError,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::IteratorPool,
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::{BufferRangeIterator, OperationsBuffer},
        iterator::SnapshotRangeIterator,
        lock::LockType,
        write::Write,
    },
    MVCCStorage, StorageCommitError,
};

macro_rules! get_mapped_method {
    ($method_name:ident, $get_func:ident) => {
        fn $method_name<T>(
            &self,
            key: StorageKeyReference<'_>,
            mut mapper: impl FnMut(&[u8]) -> T,
            storage_counters: StorageCounters,
        ) -> Result<Option<T>, SnapshotGetError> {
            let value = self.$get_func::<BUFFER_VALUE_INLINE>(key, storage_counters)?;
            Ok(value.map(|bytes| mapper(bytes.as_ref())))
        }
    };
}

pub trait ReadableSnapshot {
    fn open_sequence_number(&self) -> SequenceNumber;

    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError>;

    get_mapped_method!(get_mapped, get);

    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError>;

    get_mapped_method!(get_last_existing_mapped, get_last_existing);

    fn contains(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<bool, SnapshotGetError> {
        Ok(self.get_mapped(key, |_| (), storage_counters)?.is_some())
    }

    fn iterate_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator;

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, buffered_only: bool) -> bool;

    // --- we are slightly breaking the abstraction and Rust model by mimicking polymorphism for the following methods ---
    fn get_write(&self, key: StorageKeyReference<'_>) -> Option<&Write>;

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_;

    fn iterate_writes_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator;

    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator;

    fn iterator_pool(&self) -> &IteratorPool;
}

pub trait WritableSnapshot: ReadableSnapshot {
    fn operations(&self) -> &OperationsBuffer;

    fn operations_mut(&mut self) -> &mut OperationsBuffer;

    /// Insert a key with a new version
    fn insert(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.insert_val(key, ByteArray::empty())
    }

    fn insert_val(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.operations_mut().writes_in_mut(keyspace_id).insert(byte_array, value);
    }

    /// Insert a key with a new version
    fn uninsert(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.uninsert_val(key, ByteArray::empty())
    }

    fn uninsert_val(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.operations_mut().writes_in_mut(keyspace_id).uninsert(byte_array, value);
    }

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    fn put(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.put_val(key, ByteArray::empty())
    }

    fn put_val(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.operations_mut().writes_in_mut(keyspace_id).put(byte_array, value);
    }

    fn unput(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.unput_val(key, ByteArray::empty())
    }

    fn unput_val(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>, value: ByteArray<BUFFER_VALUE_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.operations_mut().writes_in_mut(keyspace_id).unput(byte_array, value);
    }

    /// Insert a delete marker for the key with a new version
    fn delete(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.operations_mut().writes_in_mut(keyspace_id).delete(byte_array);
    }

    /// Get a Value, and mark it as a required key
    fn get_required(
        &mut self,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) -> Result<ByteArray<BUFFER_VALUE_INLINE>, SnapshotGetError> {
        let keyspace_id = key.keyspace_id();
        let writes = self.operations().writes_in(keyspace_id);
        match writes.get(key.bytes()) {
            Some(Write::Insert { value, .. }) | Some(Write::Put { value, .. }) => Ok(ByteArray::copy(value)),
            Some(Write::Delete) => {
                Err(SnapshotGetError::ExpectedRequiredKeyToExist { key: StorageKey::Array(key.into_owned_array()) })
            }
            None => {
                let storage_value =
                    self.get_mapped(key.as_reference(), |reference| ByteArray::from(reference), storage_counters)?;
                if let Some(value) = storage_value {
                    self.operations_mut().lock_add(ByteArray::copy(key.bytes()), LockType::Unmodifiable);
                    Ok(value)
                } else {
                    Err(SnapshotGetError::ExpectedRequiredKeyToExist { key: StorageKey::Array(key.into_owned_array()) })
                }
            }
        }
    }

    // TODO: technically we should never need this in a schema txn
    fn unmodifiable_lock_add(&mut self, key: StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.operations_mut().lock_add(key.into_byte_array(), LockType::Unmodifiable)
    }

    fn unmodifiable_lock_remove(&mut self, key: &StorageKeyArray<BUFFER_KEY_INLINE>) {
        self.operations_mut().lock_remove(key.byte_array())
    }

    fn exclusive_lock_add(&mut self, key: ByteArray<BUFFER_KEY_INLINE>) {
        self.operations_mut().lock_add(key, LockType::Exclusive)
    }

    fn clear(&mut self) {
        self.operations_mut().clear()
    }

    fn close_resources(&self);
}

pub trait CommittableSnapshot<D>: WritableSnapshot
where
    D: DurabilityClient,
{
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError>;

    fn into_commit_record(self) -> CommitRecord;
}

pub struct ReadSnapshot<D> {
    open_sequence_number: SequenceNumber,
    iterator_pool: IteratorPool, // Must be declared & dropped before storage
    storage: Arc<MVCCStorage<D>>,
}

impl<D: fmt::Debug> fmt::Debug for ReadSnapshot<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).field("open_sequence_number", &self.open_sequence_number).finish()
    }
}

impl<D> ReadSnapshot<D> {
    pub(crate) fn new(storage: Arc<MVCCStorage<D>>, open_sequence_number: SequenceNumber) -> Self {
        // Note: for serialisability, we would need to register the open transaction to the IsolationManager
        ReadSnapshot { storage, open_sequence_number, iterator_pool: IteratorPool::new() }
    }

    pub fn close_resources(self) {}
}

impl<D> ReadableSnapshot for ReadSnapshot<D> {
    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        self.storage
            .get(self.iterator_pool(), key, self.open_sequence_number, storage_counters)
            .map_err(|error| SnapshotGetError::MVCCRead { source: error })
    }

    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        self.get(key, storage_counters)
    }

    fn iterate_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let mvcc_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, buffered_only: bool) -> bool {
        !buffered_only
            && self
                .storage
                .iterate_range(self.iterator_pool(), range, self.open_sequence_number, StorageCounters::DISABLED)
                .next()
                .is_some()
    }

    fn get_write(&self, _: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        empty()
    }

    fn iterate_writes_range<const PS: usize>(&self, _: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let mvcc_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}

pub struct WriteSnapshot<D> {
    operations: OperationsBuffer,
    open_sequence_number: SequenceNumber,
    iterator_pool: IteratorPool, // Pool must be declared & dropped before storage
    storage: Arc<MVCCStorage<D>>,
}

impl<D: fmt::Debug> fmt::Debug for WriteSnapshot<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).field("open_sequence_number", &self.open_sequence_number).finish()
    }
}

impl<D> WriteSnapshot<D> {
    pub(crate) fn new(storage: Arc<MVCCStorage<D>>, open_sequence_number: SequenceNumber) -> Self {
        storage.isolation_manager.opened_for_read(open_sequence_number);
        WriteSnapshot {
            storage,
            operations: OperationsBuffer::new(),
            open_sequence_number,
            iterator_pool: IteratorPool::new(),
        }
    }

    pub fn new_with_operations(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
        operations: OperationsBuffer,
    ) -> impl ReadableSnapshot {
        WriteSnapshot { storage, operations, open_sequence_number, iterator_pool: IteratorPool::new() }
    }
}

impl<D> ReadableSnapshot for WriteSnapshot<D> {
    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        let writes = self.operations().writes_in(key.keyspace_id());
        match writes.get(key.bytes()) {
            Some(Write::Insert { value, .. }) | Some(Write::Put { value, .. }) => Ok(Some(ByteArray::copy(value))),
            Some(Write::Delete) => Ok(None),
            None => self
                .storage
                .get(self.iterator_pool(), key, self.open_sequence_number, storage_counters)
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
        }
    }

    /// Get the last existing Value for the key, returning an empty Option if it did not exist
    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        let writes = self.operations().writes_in(key.keyspace_id());
        match writes.get(key.bytes()) {
            Some(Write::Insert { value, .. }) | Some(Write::Put { value, .. }) => Ok(Some(ByteArray::copy(value))),
            Some(Write::Delete) | None => self
                .storage
                .get(self.iterator_pool(), key, self.open_sequence_number, storage_counters)
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
        }
    }

    fn iterate_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let buffered_iterator = self
            .operations
            .writes_in(range.start().get_value().keyspace_id())
            .iterate_range(range.clone().map(|k| k.as_bytes(), |fixed| fixed));
        let storage_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(storage_iterator, Some(buffered_iterator))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, buffered_only: bool) -> bool {
        let buffered = self
            .operations
            .writes_in(range.start().get_value().keyspace_id())
            .any_not_deleted_in_range(range.clone().map(|k| k.as_bytes(), |fixed| fixed));
        buffered || (!buffered_only && self.iterate_range(range, StorageCounters::DISABLED).next().is_some())
    }

    fn get_write(&self, key: StorageKeyReference<'_>) -> Option<&Write> {
        self.operations().writes_in(key.keyspace_id()).get_write(key.bytes())
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        self.operations().iterate_writes()
    }

    fn iterate_writes_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        debug_assert!(range
            .end()
            .get_value()
            .map(|end| end.keyspace_id() == range.start().get_value().keyspace_id())
            .unwrap_or(true));
        self.operations()
            .writes_in(range.start().get_value().keyspace_id())
            .iterate_range(range.map(|k| k.as_bytes(), |fixed| fixed))
    }

    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let mvcc_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}

impl<D> WritableSnapshot for WriteSnapshot<D> {
    fn operations(&self) -> &OperationsBuffer {
        &self.operations
    }

    fn operations_mut(&mut self) -> &mut OperationsBuffer {
        &mut self.operations
    }

    fn close_resources(&self) {
        self.storage.closed_snapshot_write(self.open_sequence_number());
    }
}

impl<D: DurabilityClient> CommittableSnapshot<D> for WriteSnapshot<D> {
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError> {
        if self.operations.is_writes_empty() && self.operations.locks_empty() {
            Ok(None)
        } else {
            match self.storage.clone().snapshot_commit(self, commit_profile) {
                Ok(sequence_number) => Ok(Some(sequence_number)),
                Err(error) => Err(SnapshotError::Commit { typedb_source: error }),
            }
        }
    }

    fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(self.operations, self.open_sequence_number, CommitType::Data)
    }
}

pub struct SchemaSnapshot<D> {
    operations: OperationsBuffer,
    open_sequence_number: SequenceNumber,
    iterator_pool: IteratorPool, // Must be declared & dropped before storage
    storage: Arc<MVCCStorage<D>>,
}

impl<D: fmt::Debug> fmt::Debug for SchemaSnapshot<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).field("open_sequence_number", &self.open_sequence_number).finish()
    }
}

impl<D> SchemaSnapshot<D> {
    pub(crate) fn new(storage: Arc<MVCCStorage<D>>, open_sequence_number: SequenceNumber) -> Self {
        storage.isolation_manager.opened_for_read(open_sequence_number);
        SchemaSnapshot {
            storage,
            operations: OperationsBuffer::new(),
            open_sequence_number,
            iterator_pool: IteratorPool::new(),
        }
    }

    pub fn new_with_operations(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
        operations: OperationsBuffer,
    ) -> impl ReadableSnapshot {
        SchemaSnapshot { storage, operations, open_sequence_number, iterator_pool: IteratorPool::new() }
    }
}

impl<D> ReadableSnapshot for SchemaSnapshot<D> {
    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        let writes = self.operations().writes_in(key.keyspace_id());
        match writes.get(key.bytes()) {
            Some(Write::Insert { value, .. }) | Some(Write::Put { value, .. }) => Ok(Some(ByteArray::copy(value))),
            Some(Write::Delete) => Ok(None),
            None => self
                .storage
                .get(self.iterator_pool(), key, self.open_sequence_number, storage_counters)
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
        }
    }

    /// Get the last existing Value for the key, returning an empty Option if it did not exist
    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        let writes = self.operations().writes_in(key.keyspace_id());
        match writes.get(key.bytes()) {
            Some(Write::Insert { value, .. }) | Some(Write::Put { value, .. }) => Ok(Some(ByteArray::copy(value))),
            Some(Write::Delete) | None => self
                .storage
                .get(self.iterator_pool(), key, self.open_sequence_number, storage_counters)
                .map_err(|error| SnapshotGetError::MVCCRead { source: error }),
        }
    }

    fn iterate_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let buffered_iterator = self
            .operations
            .writes_in(range.start().get_value().keyspace_id())
            .iterate_range(range.clone().map(|k| k.as_bytes(), |fixed| fixed));
        let storage_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(storage_iterator, Some(buffered_iterator))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, buffered_only: bool) -> bool {
        let buffered = self
            .operations
            .writes_in(range.start().get_value().keyspace_id())
            .any_not_deleted_in_range(range.clone().map(|k| k.as_bytes(), |fixed| fixed));
        buffered || (!buffered_only && self.iterate_range(range, StorageCounters::DISABLED).next().is_some())
    }

    fn get_write(&self, key: StorageKeyReference<'_>) -> Option<&Write> {
        self.operations().writes_in(key.keyspace_id()).get_write(key.bytes())
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        self.operations().iterate_writes()
    }

    fn iterate_writes_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        debug_assert!(range
            .end()
            .get_value()
            .map(|end| end.keyspace_id() == range.start().get_value().keyspace_id())
            .unwrap_or(true));
        self.operations()
            .writes_in(range.start().get_value().keyspace_id())
            .iterate_range(range.map(|k| k.as_bytes(), |fixed| fixed))
    }

    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let mvcc_iterator =
            self.storage.iterate_range(self.iterator_pool(), range, self.open_sequence_number, storage_counters);
        SnapshotRangeIterator::new(mvcc_iterator, None)
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}

impl<D> WritableSnapshot for SchemaSnapshot<D> {
    fn operations(&self) -> &OperationsBuffer {
        &self.operations
    }

    fn operations_mut(&mut self) -> &mut OperationsBuffer {
        &mut self.operations
    }

    fn close_resources(&self) {
        self.storage.closed_snapshot_write(self.open_sequence_number());
    }
}

impl<D: DurabilityClient> CommittableSnapshot<D> for SchemaSnapshot<D> {
    // TODO: extract these two methods into separate trait
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError> {
        if self.operations.is_writes_empty() && self.operations.locks_empty() {
            Ok(None)
        } else {
            match self.storage.clone().snapshot_commit(self, commit_profile) {
                Ok(sequence_number) => Ok(Some(sequence_number)),
                Err(error) => Err(SnapshotError::Commit { typedb_source: error }),
            }
        }
    }

    fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(self.operations, self.open_sequence_number, CommitType::Schema)
    }
}

typedb_error! {
    pub SnapshotError(component = "Snapshot error", prefix = "SST") {
        Commit(1, "Snapshot commit failed due to storage commit error.", typedb_source: StorageCommitError),
    }
}

#[derive(Debug, Clone)]
pub enum SnapshotGetError {
    MVCCRead { source: MVCCReadError },
    ExpectedRequiredKeyToExist { key: StorageKey<'static, BUFFER_KEY_INLINE> },
    // for tests:
    MockError {},
}

impl fmt::Display for SnapshotGetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for SnapshotGetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MVCCRead { source, .. } => Some(source),
            SnapshotGetError::ExpectedRequiredKeyToExist { .. } => None,
            SnapshotGetError::MockError { .. } => None,
        }
    }
}
