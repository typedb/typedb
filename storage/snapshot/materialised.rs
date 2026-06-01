/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! `MaterialisedSnapshot` is a read-only `ReadableSnapshot` whose contents are
//! materialised into in-memory per-keyspace `BTreeMap`s up front. All reads
//! and range scans are served from those maps without ever consulting storage.
//!
//! It is intended for code paths that read the same keyspace many times in
//! quick succession (notably `TypeCache::new` and `CommitTimeValidation::validate`,
//! which both make millions of small reads against the schema keyspace during a
//! single schema commit). One end-to-end scan of the keyspace at construction
//! replaces the repeated MVCC iterator opens that would otherwise dominate
//! commit time on large schemas.

use std::{collections::BTreeMap, iter::empty, ops::Bound, sync::Arc};

use bytes::byte_array::ByteArray;
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
    keyspace::{IteratorPool, KeyspaceId},
    sequence_number::SequenceNumber,
    snapshot::{
        buffer::BufferRangeIterator,
        iterator::SnapshotRangeIterator,
        snapshot::{ReadableSnapshot, SnapshotGetError},
        snapshot_id::SnapshotId,
        write::Write,
    },
};

/// Inclusive `(start_byte, end_byte)` range over leading-prefix bytes of keys in a
/// keyspace. See [`MaterialisedSnapshot::load_keyspace`].
pub type PrefixByteRange = (u8, u8);

const KEYSPACE_SLOTS: usize = 16;

pub struct MaterialisedSnapshot {
    open_sequence_number: SequenceNumber,
    id: SnapshotId,
    iterator_pool: IteratorPool,
    keyspaces: Box<[Option<BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>>; KEYSPACE_SLOTS]>,
}

impl MaterialisedSnapshot {
    /// Open a read snapshot at `open_sequence_number` and materialise the keys in
    /// `keyspace_id` whose leading byte falls in any of `prefix_ranges`. Each range is
    /// `(start_byte, end_byte)` inclusive. Storage is consulted only once per range.
    /// Subsequent reads against the returned snapshot are served entirely from memory.
    ///
    /// `prefix_ranges` typically comes from `Prefix::schema_byte_ranges()` (defined in
    /// the encoding crate), which derives the schema-side byte ranges of the schema
    /// keyspace directly from each prefix's `is_schema` classification.
    pub fn load_keyspace<D>(
        storage: &Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
        keyspace_id: KeyspaceId,
        prefix_ranges: &[PrefixByteRange],
    ) -> Self
    where
        D: DurabilityClient,
    {
        let source = storage.clone().open_snapshot_read_at(open_sequence_number);
        Self::load_from_snapshot(&source, keyspace_id, prefix_ranges)
    }

    /// Materialise the merged view of `keyspace_id` visible to `source` — including any
    /// buffered writes it carries — into an in-memory `BTreeMap`. Only keys whose
    /// leading byte falls in one of `prefix_ranges` are loaded; one storage iterator is
    /// opened per range.
    pub fn load_from_snapshot<S>(source: &S, keyspace_id: KeyspaceId, prefix_ranges: &[PrefixByteRange]) -> Self
    where
        S: ReadableSnapshot,
    {
        let mut keyspaces: Box<
            [Option<BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>>; KEYSPACE_SLOTS],
        > = Box::new(std::array::from_fn(|_| None));
        let mut bt: BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>> = BTreeMap::new();
        for &(start_byte, end_byte) in prefix_ranges {
            let start_bytes = [start_byte];
            let end_bytes = [end_byte];
            let start_key: StorageKey<'_, BUFFER_KEY_INLINE> =
                StorageKey::Reference(StorageKeyReference::new_raw(keyspace_id, &start_bytes));
            let end_key: StorageKey<'_, BUFFER_KEY_INLINE> =
                StorageKey::Reference(StorageKeyReference::new_raw(keyspace_id, &end_bytes));
            let range =
                KeyRange::new_variable_width(RangeStart::Inclusive(start_key), RangeEnd::EndPrefixInclusive(end_key));
            let mut it = source.iterate_range(&range, StorageCounters::DISABLED);
            while let Some(result) = it.next() {
                let (key, value) = result.expect("MaterialisedSnapshot load failed");
                bt.insert(ByteArray::copy(key.bytes()), ByteArray::copy(&*value));
            }
        }
        keyspaces[keyspace_id.0 as usize] = Some(bt);
        Self {
            open_sequence_number: source.open_sequence_number(),
            id: SnapshotId::new(),
            iterator_pool: IteratorPool::default(),
            keyspaces,
        }
    }

    fn keyspace_btree(
        &self,
        keyspace_id: KeyspaceId,
    ) -> Option<&BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>> {
        self.keyspaces.get(keyspace_id.0 as usize).and_then(|o| o.as_ref())
    }
}

fn range_start_bound<const PS: usize>(start: &RangeStart<StorageKey<'_, PS>>) -> Bound<Vec<u8>> {
    match start {
        RangeStart::Inclusive(bytes) => Bound::Included(bytes.bytes().to_vec()),
        RangeStart::ExcludeFirstWithPrefix(bytes) => Bound::Excluded(bytes.bytes().to_vec()),
        RangeStart::ExcludePrefix(bytes) => {
            let mut v = bytes.bytes().to_vec();
            increment_bytes(&mut v);
            Bound::Included(v)
        }
    }
}

fn range_end_bound<const PS: usize>(
    start: &RangeStart<StorageKey<'_, PS>>,
    end: &RangeEnd<StorageKey<'_, PS>>,
) -> Bound<Vec<u8>> {
    match end {
        RangeEnd::WithinStartAsPrefix => {
            let mut v = start.get_value().bytes().to_vec();
            increment_bytes(&mut v);
            Bound::Excluded(v)
        }
        RangeEnd::EndPrefixInclusive(value) => {
            let mut v = value.bytes().to_vec();
            increment_bytes(&mut v);
            Bound::Excluded(v)
        }
        RangeEnd::EndPrefixExclusive(value) => Bound::Excluded(value.bytes().to_vec()),
        RangeEnd::Unbounded => Bound::Unbounded,
    }
}

fn increment_bytes(v: &mut Vec<u8>) {
    for i in (0..v.len()).rev() {
        if v[i] < u8::MAX {
            v[i] += 1;
            v.truncate(i + 1);
            return;
        }
    }
    v.clear();
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
            return SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new(Vec::new()));
        };
        let start = range_start_bound(range.start());
        let end = range_end_bound(range.start(), range.end());
        let start_ref = match &start {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_ref = match &end {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let materialised: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, Write)> = bt
            .range::<[u8], _>((start_ref, end_ref))
            .map(|(k, v)| (StorageKeyArray::new_raw(keyspace_id, k.clone()), Write::Insert { value: v.clone() }))
            .collect();
        SnapshotRangeIterator::new_buffered_only(BufferRangeIterator::new(materialised))
    }

    fn any_in_range<const PS: usize>(&self, range: &KeyRange<StorageKey<'_, PS>>, _buffered_only: bool) -> bool {
        let keyspace_id = range.start().get_value().keyspace_id();
        let Some(bt) = self.keyspace_btree(keyspace_id) else { return false };
        let start = range_start_bound(range.start());
        let end = range_end_bound(range.start(), range.end());
        let start_ref = match &start {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_ref = match &end {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        bt.range::<[u8], _>((start_ref, end_ref)).next().is_some()
    }

    fn get_write(&self, _: StorageKeyReference<'_>) -> Option<&Write> {
        None
    }

    fn iterate_writes(&self) -> impl Iterator<Item = (StorageKeyArray<BUFFER_KEY_INLINE>, Write)> + '_ {
        empty()
    }

    fn iterate_writes_range<const PS: usize>(&self, _range: &KeyRange<StorageKey<'_, PS>>) -> BufferRangeIterator {
        BufferRangeIterator::new(Vec::new())
    }

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
