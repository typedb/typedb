/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::iter::empty;

use bytes::byte_array::ByteArray;
use resource::profile::StorageCounters;
use storage::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::IteratorPool,
    sequence_number::SequenceNumber,
    snapshot::{
        ReadableSnapshot, SnapshotGetError, SnapshotLookupMode, buffer::BufferRangeIterator,
        iterator::SnapshotRangeIterator, snapshot_id::SnapshotId, write::Write,
    },
};

pub struct MockSnapshot {
    id: SnapshotId,
    iterator_pool: IteratorPool,
}

impl MockSnapshot {
    pub fn new() -> Self {
        Self { id: SnapshotId::new(), iterator_pool: IteratorPool::default() }
    }
}

impl ReadableSnapshot for MockSnapshot {
    const IMMUTABLE_SCHEMA: bool = false;

    fn open_sequence_number(&self) -> SequenceNumber {
        SequenceNumber::MIN
    }

    fn id(&self) -> SnapshotId {
        self.id
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        _: StorageKeyReference<'_>,
        _storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        Err(SnapshotGetError::MockError {})
    }

    fn get_in_lookup_mode<const INLINE_BYTES: usize>(
        &self,
        _: StorageKeyReference<'_>,
        _: SnapshotLookupMode,
        _: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        Err(SnapshotGetError::MockError {})
    }

    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        _: StorageKeyReference<'_>,
        _storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        Err(SnapshotGetError::MockError {})
    }

    fn iterate_range<const PS: usize>(
        &self,
        _: &KeyRange<StorageKey<'_, PS>>,
        _: StorageCounters,
    ) -> SnapshotRangeIterator {
        SnapshotRangeIterator::new_empty()
    }

    fn iterate_range_in_lookup_mode<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        _lookup_mode: SnapshotLookupMode,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        self.iterate_range(range, storage_counters)
    }

    fn any_in_range<'this, const PS: usize>(&'this self, _: &KeyRange<StorageKey<'this, PS>>, _: bool) -> bool {
        false
    }

    fn get_write(&self, _: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes<'this>(&'this self) -> impl Iterator<Item = (StorageKeyReference<'this>, &'this Write)> + 'this {
        empty()
    }

    fn iterate_writes_range<'this, const PS: usize>(
        &'this self,
        _: &KeyRange<StorageKey<'this, PS>>,
    ) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterate_writes_range_limited<'this, const PS: usize>(
        &'this self,
        _: &KeyRange<StorageKey<'this, PS>>,
        _: usize,
    ) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}
