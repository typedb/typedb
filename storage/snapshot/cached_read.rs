/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! `CachedReadSnapshot` is a read-only `ReadableSnapshot` whose contents are
//! materialised into in-memory per-keyspace `BTreeMap`s up front. All reads and
//! range scans are served from those maps without ever consulting storage.
//!
//! It is intended for code paths that read the same keyspace ranges many times
//! in quick succession (notably `TypeCache::new` and
//! `CommitTimeValidation::validate`, which both make millions of small reads
//! against the schema keyspace during a single schema commit). One end-to-end
//! scan per `(keyspace, range)` at construction replaces the repeated MVCC
//! iterator opens that would otherwise dominate commit time on large schemas.

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
    /// Open a read snapshot at `open_sequence_number` and materialise, for each
    /// `(keyspace_id, ranges)` entry of `keyspaces_ranges`, the keys whose
    /// leading byte falls in any of `ranges`. One storage iterator is opened
    /// per range; subsequent reads against the returned snapshot are served
    /// entirely from memory.
    ///
    /// The per-keyspace `ranges` typically come from
    /// `Prefix::schema_byte_ranges()` (defined in the encoding crate), which
    /// derives the schema-side byte ranges of a keyspace directly from each
    /// prefix's `is_schema` classification.
    pub fn load_keyspaces<D>(
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

    /// Materialise the merged view (storage + buffered writes) visible to
    /// `source` for each `(keyspace_id, ranges)` entry of `keyspaces_ranges`
    /// into in-memory `BTreeMap`s. Only keys whose leading byte falls in one
    /// of the entry's `ranges` are loaded; one storage iterator is opened per
    /// range.
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
            let bt = keyspaces[keyspace_id.0 as usize].get_or_insert_with(BTreeMap::new);
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
                let mut it = source.iterate_range(&key_range, StorageCounters::DISABLED);
                while let Some(result) = it.next() {
                    let (key, value) = result.expect("CachedReadSnapshot load failed");
                    bt.insert(ByteArray::copy(key.bytes()), ByteArray::copy(&*value));
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

    fn keyspace_btree(&self, keyspace_id: KeyspaceId) -> Option<&KeyspaceBtree> {
        self.keyspaces.get(keyspace_id.0 as usize).and_then(|o| o.as_ref())
    }
}

/// Translate a `KeyRange<StorageKey>` to a pair of byte bounds suitable for a
/// `BTreeMap<ByteArray, _>::range::<[u8], _>` call. Owned `Vec<u8>` is returned
/// so the caller can borrow them as `&[u8]` via `Bound::as_ref().map(...)`.
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
        let Some(bt) = self.keyspace_btree(key.keyspace_id()) else { return Ok(None) };
        Ok(bt.get(key.bytes()).map(|v| ByteArray::copy(&**v)))
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
        let Some(bt) = self.keyspace_btree(keyspace_id) else {
            return SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new_empty());
        };
        let (start, end) = key_range_as_byte_bounds(range);
        let materialised: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, Write)> = bt
            .range::<[u8], _>((start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice)))
            .map(|(k, v)| (StorageKeyArray::new_raw(keyspace_id, k.clone()), Write::Insert { value: v.clone() }))
            .collect();
        SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new(materialised))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, _buffered_only: bool) -> bool {
        let keyspace_id = range.start().get_value().keyspace_id();
        let Some(bt) = self.keyspace_btree(keyspace_id) else { return false };
        let (start, end) = key_range_as_byte_bounds(range);
        bt.range::<[u8], _>((start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice))).next().is_some()
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
