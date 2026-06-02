/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::BTreeMap,
    iter::empty,
    ops::{Bound, RangeInclusive},
    sync::Arc,
};

use bytes::{Bytes, byte_array::ByteArray};
use error::typedb_error;
use lending_iterator::LendingIterator;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::StorageCounters,
};

use crate::{
    MVCCStorage,
    durability_client::DurabilityClient,
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{IteratorPool, KEYSPACE_MAXIMUM_COUNT, KeyspaceId},
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::{BufferRangeIterator, compute_exclusive_end, range_start_as_bound},
        iterator::{SnapshotIteratorError, SnapshotRangeIterator},
        snapshot::{ReadableSnapshot, SnapshotGetError},
        snapshot_id::SnapshotId,
        write::Write,
    },
};

type KeyspaceBtree = BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>;

pub struct CachedReadSnapshot {
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
    iterator_pool: IteratorPool,
    keyspaces: Box<[Option<KeyspaceBtree>; KEYSPACE_MAXIMUM_COUNT]>,
}

impl CachedReadSnapshot {
    pub fn load_at<D>(
        storage: &Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
        keyspaces_ranges: Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>,
    ) -> Result<Self, CachedReadSnapshotLoadError>
    where
        D: DurabilityClient,
    {
        let source = storage.clone().open_snapshot_read_at(open_sequence_number);
        Self::load_from_snapshot(&source, keyspaces_ranges)
    }

    pub fn load_from_snapshot<S>(
        source: &S,
        keyspaces_ranges: Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>,
    ) -> Result<Self, CachedReadSnapshotLoadError>
    where
        S: ReadableSnapshot,
    {
        let mut keyspaces: Box<[Option<KeyspaceBtree>; KEYSPACE_MAXIMUM_COUNT]> =
            Box::new(std::array::from_fn(|_| None));
        for (keyspace_id, ranges) in keyspaces_ranges {
            let map = keyspaces[keyspace_id.0 as usize].get_or_insert_with(BTreeMap::new);
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
                    let (key, value) = result.map_err(|source| CachedReadSnapshotLoadError::Iterate { source })?;
                    map.insert(key.into_bytes().into_array(), value.into_array());
                }
            }
        }
        Ok(Self {
            open_sequence_number: source.open_sequence_number(),
            id: SnapshotId::new(),
            iterator_pool: IteratorPool::default(),
            keyspaces,
        })
    }

    fn keyspace_map(&self, keyspace_id: KeyspaceId) -> Option<&KeyspaceBtree> {
        self.keyspaces.get(keyspace_id.0 as usize).and_then(|o| o.as_ref())
    }
}

impl ReadableSnapshot for CachedReadSnapshot {
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

    /// This snapshot has no buffered writes — its contents are the merged view
    /// of (storage + buffered writes) captured at load time, baked into the
    /// in-memory `BTreeMap`s. As such there is no separate storage-only view to
    /// expose, and this method returns the same iterator as `iterate_range`.
    fn iterate_storage_range<const PS: usize>(
        &self,
        range: &KeyRange<StorageKey<'_, PS>>,
        storage_counters: StorageCounters,
    ) -> SnapshotRangeIterator {
        self.iterate_range(range, storage_counters)
    }

    fn iterator_pool(&self) -> &IteratorPool {
        &self.iterator_pool
    }
}

typedb_error! {
    pub CachedReadSnapshotLoadError(component = "Cached read snapshot load", prefix = "CRS") {
        Iterate(1, "Failed to materialise a key range into the cached read snapshot.", source: Arc<SnapshotIteratorError>),
    }
}
