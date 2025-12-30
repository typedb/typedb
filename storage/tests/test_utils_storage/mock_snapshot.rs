/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{iter::empty, marker::PhantomData};

use bytes::byte_array::ByteArray;
use kv::KVStore;
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;
use storage::{
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::BufferRangeIterator, iterator::SnapshotRangeIterator, write::Write, ReadableSnapshot, SnapshotGetError,
    },
};

pub struct MockSnapshot<KV: KVStore> {
    _marker: PhantomData<KV>,
}

impl<KV: KVStore> Default for MockSnapshot<KV> {
    fn default() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<KV: KVStore> MockSnapshot<KV> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<KV: KVStore> ReadableSnapshot<KV> for MockSnapshot<KV> {
    const IMMUTABLE_SCHEMA: bool = false;

    fn open_sequence_number(&self) -> SequenceNumber {
        SequenceNumber::MIN
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        _: StorageKeyReference<'_>,
        _storage_counters: StorageCounters,
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
    ) -> SnapshotRangeIterator<KV> {
        SnapshotRangeIterator::new_empty()
    }

    fn any_in_range<const PS: usize>(&self, _: &KeyRange<StorageKey<'_, PS>>, _: bool) -> bool {
        false
    }

    fn get_write(&self, _: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes(
        &self,
    ) -> impl Iterator<Item = (StorageKeyArray<{ resource::constants::snapshot::BUFFER_KEY_INLINE }>, Write)> + '_ {
        empty()
    }

    fn iterate_writes_range<const PS: usize>(&self, _: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        BufferRangeIterator::new_empty()
    }

    fn iterate_storage_range<const PS: usize>(
        &self,
        _: &KeyRange<StorageKey<'_, PS>>,
        _: StorageCounters,
    ) -> SnapshotRangeIterator<KV> {
        SnapshotRangeIterator::new_empty()
    }

    fn close_resources(&self) {}
}
