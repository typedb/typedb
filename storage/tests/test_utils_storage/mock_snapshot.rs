/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::iter::empty;

use bytes::byte_array::ByteArray;
use storage::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::IteratorPool,
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::BufferRangeIterator, iterator::SnapshotRangeIterator, write::Write, ReadableSnapshot, SnapshotGetError,
    },
};

pub struct MockSnapshot {
    iterator_pool: IteratorPool,
}

impl MockSnapshot {
    pub fn new() -> Self {
        Self { iterator_pool: IteratorPool::new() }
    }
}

impl ReadableSnapshot for MockSnapshot {
    fn open_sequence_number(&self) -> SequenceNumber {
        SequenceNumber::MIN
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        Err(SnapshotGetError::MockError {})
    }

    fn get_last_existing<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, SnapshotGetError> {
        Err(SnapshotGetError::MockError {})
    }

    fn iterate_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>) -> SnapshotRangeIterator {
        SnapshotRangeIterator::new_empty()
    }

    fn any_in_range<'this, const PS: usize>(
        &'this self,
        range: &KeyRange<StorageKey<'this, PS>>,
        buffered_only: bool,
    ) -> bool {
        false
    }

    fn get_write(&self, key: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes(
        &self,
    ) -> impl Iterator<Item = (StorageKeyArray<{ resource::constants::snapshot::BUFFER_KEY_INLINE }>, Write)> + '_ {
        empty()
    }

    fn iterate_writes_range<'this, const PS: usize>(
        &'this self,
        range: &KeyRange<StorageKey<'this, PS>>,
    ) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterate_storage_range<'this, const PS: usize>(
        &'this self,
        range: &KeyRange<StorageKey<'this, PS>>,
    ) -> SnapshotRangeIterator {
        SnapshotRangeIterator::new_empty()
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}
