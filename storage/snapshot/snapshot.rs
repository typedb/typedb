/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::type_name,
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
    iter::empty,
    ops::{Bound, RangeInclusive},
    sync::Arc,
};

use bytes::{Bytes, byte_array::ByteArray};
use error::typedb_error;
use lending_iterator::LendingIterator;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::{CommitProfile, StorageCounters},
};

use crate::{
    MVCCStorage, StorageCommitError,
    durability_client::DurabilityClient,
    isolation_manager::ReaderDropGuard,
    iterator::MVCCReadError,
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceId},
    record::{CommitRecord, CommitType},
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::{BufferRangeIterator, OperationsBuffer, compute_exclusive_end, range_start_as_bound},
        iterator::{SnapshotIteratorError, SnapshotRangeIterator},
        lock::LockType,
        snapshot_id::SnapshotId,
        write::Write,
    },
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
    const IMMUTABLE_SCHEMA: bool;

    fn open_sequence_number(&self) -> SequenceNumber;

    fn id(&self) -> SnapshotId;

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
        match writes.writes_get(key.bytes()) {
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
}

pub trait CommittableSnapshot<D>: WritableSnapshot
where
    D: DurabilityClient,
{
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError>;

    fn into_commit_record(self) -> (ReaderDropGuard, CommitRecord);

    fn has_changes(&self) -> bool {
        !self.operations().is_writes_empty() || !self.operations().locks_empty()
    }
}

pub struct ReadSnapshot<D> {
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
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
        ReadSnapshot { open_sequence_number, id: SnapshotId::new(), iterator_pool: IteratorPool::new(), storage }
    }
}

impl<D> ReadableSnapshot for ReadSnapshot<D> {
    const IMMUTABLE_SCHEMA: bool = true;

    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn id(&self) -> SnapshotId {
        self.id
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
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
    operations: OperationsBuffer,
    iterator_pool: IteratorPool, // Pool must be declared & dropped before storage
    storage: Arc<MVCCStorage<D>>,
    reader_guard: ReaderDropGuard,
}

impl<D: fmt::Debug> fmt::Debug for WriteSnapshot<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("open_sequence_number", &self.open_sequence_number)
            .field("id", &self.id)
            .finish()
    }
}

impl<D> WriteSnapshot<D> {
    pub(crate) fn new_with_open_sequence_number(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
    ) -> Self {
        Self::new(storage, OperationsBuffer::new(), open_sequence_number, None)
    }

    pub fn new_with_commit_record(storage: Arc<MVCCStorage<D>>, commit_record: CommitRecord) -> Self {
        let open_sequence_number = commit_record.open_sequence_number();
        let id = Some(commit_record.snapshot_id());
        Self::new(storage, commit_record.into_operations(), open_sequence_number, id)
    }

    fn new(
        storage: Arc<MVCCStorage<D>>,
        operations: OperationsBuffer,
        open_sequence_number: SequenceNumber,
        id: Option<SnapshotId>,
    ) -> Self {
        let reader_guard = storage.isolation_manager.opened_for_read(open_sequence_number);
        WriteSnapshot {
            storage,
            operations,
            open_sequence_number,
            id: id.unwrap_or_else(|| SnapshotId::new()),
            iterator_pool: IteratorPool::new(),
            reader_guard,
        }
    }
}

impl<D> ReadableSnapshot for WriteSnapshot<D> {
    const IMMUTABLE_SCHEMA: bool = true;

    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn id(&self) -> SnapshotId {
        self.id
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        match self.get_write(key) {
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
        match self.get_write(key) {
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
        self.operations().writes_in(key.keyspace_id()).writes_get(key.bytes())
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        self.operations().iterate_writes()
    }

    fn iterate_writes_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        debug_assert!(
            range
                .end()
                .get_value()
                .map(|end| end.keyspace_id() == range.start().get_value().keyspace_id())
                .unwrap_or(true)
        );
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
}

impl<D: DurabilityClient> CommittableSnapshot<D> for WriteSnapshot<D> {
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError> {
        if self.has_changes() {
            self.storage
                .clone()
                .snapshot_commit(self, commit_profile)
                .map(Some)
                .map_err(|typedb_source| SnapshotError::Commit { typedb_source })
        } else {
            Ok(None)
        }
    }

    fn into_commit_record(self) -> (ReaderDropGuard, CommitRecord) {
        let Self { operations, open_sequence_number, id, reader_guard, iterator_pool: _, storage: _ } = self;
        (reader_guard, CommitRecord::new(operations, open_sequence_number, CommitType::Data, id))
    }
}

pub struct SchemaSnapshot<D> {
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
    operations: OperationsBuffer,
    iterator_pool: IteratorPool, // Must be declared & dropped before storage
    storage: Arc<MVCCStorage<D>>,
    reader_guard: ReaderDropGuard,
}

impl<D: fmt::Debug> fmt::Debug for SchemaSnapshot<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("open_sequence_number", &self.open_sequence_number)
            .field("id", &self.id)
            .finish()
    }
}

impl<D> SchemaSnapshot<D> {
    pub(crate) fn new_with_open_sequence_number(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
    ) -> Self {
        Self::new(storage, OperationsBuffer::new(), open_sequence_number, None)
    }

    pub fn new_with_commit_record(storage: Arc<MVCCStorage<D>>, commit_record: CommitRecord) -> Self {
        let open_sequence_number = commit_record.open_sequence_number();
        let id = Some(commit_record.snapshot_id());
        Self::new(storage, commit_record.into_operations(), open_sequence_number, id)
    }

    fn new(
        storage: Arc<MVCCStorage<D>>,
        operations: OperationsBuffer,
        open_sequence_number: SequenceNumber,
        id: Option<SnapshotId>,
    ) -> Self {
        let reader_guard = storage.isolation_manager.opened_for_read(open_sequence_number);
        SchemaSnapshot {
            storage,
            operations,
            open_sequence_number,
            id: id.unwrap_or_else(|| SnapshotId::new()),
            iterator_pool: IteratorPool::new(),
            reader_guard,
        }
    }
}

impl<D> ReadableSnapshot for SchemaSnapshot<D> {
    const IMMUTABLE_SCHEMA: bool = false;

    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn id(&self) -> SnapshotId {
        self.id
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        match self.get_write(key) {
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
        match self.get_write(key) {
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
        self.operations().writes_in(key.keyspace_id()).writes_get(key.bytes())
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        self.operations().iterate_writes()
    }

    fn iterate_writes_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        debug_assert!(
            range
                .end()
                .get_value()
                .map(|end| end.keyspace_id() == range.start().get_value().keyspace_id())
                .unwrap_or(true)
        );
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
}

impl<D: DurabilityClient> CommittableSnapshot<D> for SchemaSnapshot<D> {
    fn commit(self, commit_profile: &mut CommitProfile) -> Result<Option<SequenceNumber>, SnapshotError> {
        if self.has_changes() {
            self.storage
                .clone()
                .snapshot_commit(self, commit_profile)
                .map(Some)
                .map_err(|typedb_source| SnapshotError::Commit { typedb_source })
        } else {
            Ok(None)
        }
    }

    fn into_commit_record(self) -> (ReaderDropGuard, CommitRecord) {
        let Self { operations, open_sequence_number, reader_guard, id, iterator_pool: _, storage: _ } = self;
        (reader_guard, CommitRecord::new(operations, open_sequence_number, CommitType::Schema, id))
    }
}

type KeyspaceBtree = BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>;

/// A read-only snapshot whose contents have been pre-materialised into in-memory
/// per-keyspace `BTreeMap`s at construction time. Every subsequent `get`,
/// `iterate_range`, and `any_in_range` is served from those maps with no MVCC
/// or rocksdb traffic, which is orders of magnitude cheaper on hot paths that
/// re-read the same keyspace millions of times during a single transaction
/// (notably `TypeCache::new` and `CommitTimeValidation::validate`).
///
/// The constructors take a `Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>` so only
/// the keyspaces and leading-byte ranges the caller actually plans to read are
/// loaded; any other keyspace queried at runtime is a programming error
/// (`debug_assert`-checked).
///
/// When constructed via `load_from_snapshot` over a writable source, the merged
/// (storage + buffered) view visible to the source at wrap time is baked in;
/// mutations to the source after wrapping are *not* reflected.
pub struct MaterialisedSnapshot {
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
    keyspaces: HashMap<KeyspaceId, KeyspaceBtree>,
}

impl MaterialisedSnapshot {
    pub fn load_from<D>(
        storage: &Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
        keyspaces_ranges: Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>,
    ) -> Result<Self, Arc<SnapshotIteratorError>>
    where
        D: DurabilityClient,
    {
        let source = storage.clone().open_snapshot_read_at(open_sequence_number);
        Self::load_from_snapshot(&source, keyspaces_ranges)
    }

    pub fn load_from_snapshot<S>(
        source: &S,
        keyspaces_ranges: Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>,
    ) -> Result<Self, Arc<SnapshotIteratorError>>
    where
        S: ReadableSnapshot,
    {
        let mut keyspaces: HashMap<KeyspaceId, KeyspaceBtree> = HashMap::new();
        for (keyspace_id, ranges) in keyspaces_ranges {
            let map = keyspaces.entry(keyspace_id).or_default();
            for range in ranges {
                let start_bytes = [*range.start()];
                let end_bytes = [*range.end()];
                let start_key: StorageKey<'_, BUFFER_KEY_INLINE> =
                    StorageKey::Reference(StorageKeyReference::new_raw(keyspace_id, &start_bytes));
                let end_key: StorageKey<'_, BUFFER_KEY_INLINE> =
                    StorageKey::Reference(StorageKeyReference::new_raw(keyspace_id, &end_bytes));
                let key_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(start_key),
                    RangeEnd::EndPrefixInclusive(end_key),
                );
                let mut iterator = source.iterate_range(&key_range, StorageCounters::DISABLED);
                while let Some(result) = iterator.next() {
                    let (key, value) = result?;
                    map.insert(key.into_bytes().into_array(), value.into_array());
                }
            }
        }
        Ok(Self {
            open_sequence_number: source.open_sequence_number(),
            id: SnapshotId::new(),
            keyspaces,
        })
    }

    fn keyspace_map(&self, keyspace_id: KeyspaceId) -> Option<&KeyspaceBtree> {
        debug_assert!(
            self.keyspaces.contains_key(&keyspace_id),
            "MaterialisedSnapshot asked for keyspace {:?}, which was not materialised at load time",
            keyspace_id,
        );
        self.keyspaces.get(&keyspace_id)
    }
}

impl ReadableSnapshot for MaterialisedSnapshot {
    const IMMUTABLE_SCHEMA: bool = true;

    fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn id(&self) -> SnapshotId {
        self.id
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        _storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        let Some(map) = self.keyspace_map(key.keyspace_id()) else { return Ok(None) };
        Ok(map.get(key.bytes()).map(|v| ByteArray::copy(&**v)))
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
        _storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        let keyspace_id = range.start().get_value().keyspace_id();
        let Some(map) = self.keyspace_map(keyspace_id) else {
            return SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new_empty());
        };
        let (range_start, range_end, _) = range.map(|sk| sk.as_bytes(), |w| w).into_raw();
        let exclusive_end_bytes = compute_exclusive_end(&range_start, &range_end);
        let end = if matches!(range_end, RangeEnd::Unbounded) {
            Bound::Unbounded
        } else {
            Bound::Excluded(&*exclusive_end_bytes)
        };
        let start_as_bound = range_start_as_bound(range_start);
        let start_bytes = start_as_bound.as_ref().map(|bytes| bytes.as_ref());
        let materialised: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, Write)> = map
            .range::<[u8], _>((start_bytes, end))
            .map(|(k, v)| (StorageKeyArray::new_raw(keyspace_id, k.clone()), Write::Insert { value: v.clone() }))
            .collect();
        SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new(materialised))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, _buffered_only: bool) -> bool {
        let keyspace_id = range.start().get_value().keyspace_id();
        let Some(map) = self.keyspace_map(keyspace_id) else { return false };
        let (range_start, range_end, _) = range.map(|sk| sk.as_bytes(), |w| w).into_raw();
        let exclusive_end_bytes = compute_exclusive_end(&range_start, &range_end);
        let end = if matches!(range_end, RangeEnd::Unbounded) {
            Bound::Unbounded
        } else {
            Bound::Excluded(&*exclusive_end_bytes)
        };
        let start_as_bound = range_start_as_bound(range_start);
        let start_bytes = start_as_bound.as_ref().map(|bytes| bytes.as_ref());
        map.range::<[u8], _>((start_bytes, end)).next().is_some()
    }

    fn get_write(&self, _: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        empty()
    }

    fn iterate_writes_range<const PS: usize>(&self, _range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        self.iterate_range(range, storage_counters)
    }

    fn iterator_pool(&self) -> &IteratorPool {
        // No MVCC iterators are ever opened against this snapshot — all reads
        // are served from the in-memory BTreeMaps materialised at load time.
        // The trait requires this method, so we hand out a thread-shared empty
        // pool rather than carry a dead one per snapshot instance.
        static EMPTY_POOL: std::sync::OnceLock<IteratorPool> = std::sync::OnceLock::new();
        EMPTY_POOL.get_or_init(IteratorPool::default)
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
