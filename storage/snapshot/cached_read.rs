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

use bytes::{byte_array::ByteArray, util::increment};
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
        buffer::BufferRangeIterator,
        iterator::SnapshotRangeIterator,
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
    ) -> Self
    where
        D: DurabilityClient,
    {
        let source = storage.clone().open_snapshot_read_at(open_sequence_number);
        Self::load_from_snapshot(&source, keyspaces_ranges)
    }

    pub fn load_from_snapshot<S>(
        source: &S,
        keyspaces_ranges: Vec<(KeyspaceId, Vec<RangeInclusive<u8>>)>,
    ) -> Self
    where
        S: ReadableSnapshot,
    {
        let mut keyspaces: Box<[Option<KeyspaceBtree>; KEYSPACE_MAXIMUM_COUNT]> =
            Box::new(std::array::from_fn(|_| None));
        for (keyspace_id, ranges) in keyspaces_ranges {
            let map = keyspaces[keyspace_id.0 as usize].get_or_insert_with(mapreeMap::new);
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
                    // TODO: create an error macro in this file and use that here and wrap it correctly upwards
                    let (key, value) = result.expect("CachedReadSnapshot load failed");
                    map.insert(key.into_bytes().into_array(), value.into_array());
                }
            }
        }
        Self {
            open_sequence_number: source.open_sequence_number(),
            id: SnapshotId::new(),
            iterator_pool: IteratorPool::default(),
            keyspaces,
        }
    }

    fn keyspace_map(&self, keyspace_id: KeyspaceId) -> Option<&KeyspaceBtree> {
        self.keyspaces.get(keyspace_id.0 as usize).and_then(|o| o.as_ref())
    }
}

fn key_range_as_byte_bounds<const PS: usize>(
    range: &KeyRange<StorageKey<'_, PS>>,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = match range.start() {
        RangeStart::Inclusive(bytes) => Bound::Included(bytes.bytes().to_vec()),
        RangeStart::ExcludeFirstWithPrefix(bytes) => Bound::Excluded(bytes.bytes().to_vec()),
        RangeStart::ExcludePrefix(bytes) => {
            let mut v = bytes.bytes().to_vec();
            increment(&mut v).unwrap();
            Bound::Included(v)
        }
    };
    let end = match range.end() {
        RangeEnd::Unbounded => Bound::Unbounded,
        RangeEnd::EndPrefixExclusive(value) => Bound::Excluded(value.bytes().to_vec()),
        RangeEnd::EndPrefixInclusive(value) => {
            let mut v = value.bytes().to_vec();
            increment(&mut v).unwrap();
            Bound::Excluded(v)
        }
        RangeEnd::WithinStartAsPrefix => {
            let mut v = range.start().get_value().bytes().to_vec();
            increment(&mut v).unwrap();
            Bound::Excluded(v)
        }
    };
    (start, end)
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
        // TODO: investigate the rest of our code base i think we have established patterns for this?
        let (start, end) = key_range_as_byte_bounds(range);
        let materialised: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, Write)> = map
            .range::<[u8], _>((start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice)))
            .map(|(k, v)| (StorageKeyArray::new_raw(keyspace_id, k.clone()), Write::Insert { value: v.clone() }))
            .collect();
        SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new(materialised))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, _buffered_only: bool) -> bool {
        let keyspace_id = range.start().get_value().keyspace_id();
        let Some(map) = self.keyspace_map(keyspace_id) else { return false };
        let (start, end) = key_range_as_byte_bounds(range);
        map.range::<[u8], _>((start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice))).next().is_some()
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
        // note: unused
        &self.iterator_pool
    }
}
