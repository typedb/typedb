/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fmt,
    hash::Hash,
    sync::Arc,
};

use bytes::{Bytes, byte_array::ByteArray};
use lending_iterator::{LendingIterator, Seekable};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};

use crate::{
    iterator::{MVCCRangeIterator, MVCCReadError},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::{buffer::BufferRangeIterator, write::Write},
};

pub struct SnapshotRangeIterator {
    sources: Sources,
}

/// The sources an iterator still has items from. Buffered ranges are copied when the iterator is created, so a merge
/// never needs to restart once either side is exhausted: it continues with the other side alone.
#[derive(Default)]
enum Sources {
    #[default]
    Empty,
    Storage(MVCCRangeIterator),
    Buffered(BufferRangeIterator),
    Merged {
        storage: MVCCRangeIterator,
        buffered: BufferRangeIterator,
        // which side holds the next item, once resolved by `resolve_merge`
        ready_item_source: Option<ReadyItemSource>,
    },
}

impl Sources {
    fn without_storage(self) -> Self {
        match self {
            Sources::Merged { buffered, .. } => Sources::Buffered(buffered),
            Sources::Storage(_) => Sources::Empty,
            sources => sources,
        }
    }

    fn without_buffered(self) -> Self {
        match self {
            Sources::Merged { storage, .. } => Sources::Storage(storage),
            Sources::Buffered(_) => Sources::Empty,
            sources => sources,
        }
    }
}

impl SnapshotRangeIterator {
    pub(crate) fn new(mvcc_iterator: MVCCRangeIterator, buffered_iterator: Option<BufferRangeIterator>) -> Self {
        let sources = match buffered_iterator {
            Some(buffered) if !buffered.is_empty() => {
                Sources::Merged { storage: mvcc_iterator, buffered, ready_item_source: None }
            }
            _ => Sources::Storage(mvcc_iterator),
        };
        SnapshotRangeIterator { sources }
    }

    pub fn new_buffered_only(buffered_iterator: BufferRangeIterator) -> Self {
        SnapshotRangeIterator { sources: Sources::Buffered(buffered_iterator) }
    }

    pub fn new_empty() -> Self {
        SnapshotRangeIterator { sources: Sources::Empty }
    }

    pub fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), Arc<SnapshotIteratorError>>> {
        self.resolve_merge();
        match &mut self.sources {
            Sources::Empty => None,
            Sources::Storage(storage) => Self::storage_peek(storage),
            Sources::Buffered(buffered) => Self::buffered_peek(buffered),
            Sources::Merged { storage, buffered, ready_item_source } => {
                match ready_item_source.expect("resolved by resolve_merge") {
                    ReadyItemSource::Storage => Self::storage_peek(storage),
                    ReadyItemSource::Buffered | ReadyItemSource::Both => Self::buffered_peek(buffered),
                }
            }
        }
    }

    pub fn seek(&mut self, key: StorageKeyReference<'_>) {
        // all sources check that the seek is in ascending order internally, so seeking both sides of a merge is
        // a no-op for any side that is already at or past the key
        match &mut self.sources {
            Sources::Empty => (),
            Sources::Storage(storage) => storage.seek(key.bytes()),
            Sources::Buffered(buffered) => buffered.seek(key.bytes()),
            Sources::Merged { storage, buffered, ready_item_source } => {
                *ready_item_source = None;
                buffered.seek(key.bytes());
                storage.seek(key.bytes());
            }
        }
    }

    /// For merged sources, determines which side holds the next item, skipping buffered deletes and the storage items
    /// they delete. Continues with a single source once either side is exhausted.
    #[inline]
    fn resolve_merge(&mut self) {
        if matches!(self.sources, Sources::Merged { ready_item_source: None, .. }) {
            self.resolve_merge_sources();
        }
    }

    fn resolve_merge_sources(&mut self) {
        while let Sources::Merged { storage, buffered, ready_item_source: ready_item_source @ None } = &mut self.sources
        {
            let storage_key = match storage.peek() {
                None => {
                    self.sources = std::mem::take(&mut self.sources).without_storage();
                    return;
                }
                Some(Err(_)) => {
                    *ready_item_source = Some(ReadyItemSource::Storage);
                    return;
                }
                Some(Ok((storage_key, _))) => storage_key,
            };
            let Some((buffered_key, buffered_write)) = buffered.peek() else {
                self.sources = std::mem::take(&mut self.sources).without_buffered();
                return;
            };
            let is_delete = buffered_write.is_delete();
            match StorageKeyReference::from(buffered_key).cmp(&storage_key) {
                Ordering::Less if is_delete => {
                    // SKIP buffered
                    buffered.next();
                }
                Ordering::Less => *ready_item_source = Some(ReadyItemSource::Buffered),
                Ordering::Equal if is_delete => {
                    // SKIP both
                    storage.next();
                    buffered.next();
                }
                Ordering::Equal => *ready_item_source = Some(ReadyItemSource::Both),
                Ordering::Greater => *ready_item_source = Some(ReadyItemSource::Storage),
            }
        }
    }

    pub fn collect_cloned_vec<F, M>(self, mapper: F) -> Result<Vec<M>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, &'b [u8]) -> M + 'static,
        M: 'static,
    {
        self.map_static(move |res| res.map(|(a, b)| mapper(a.as_reference(), &b))).collect()
    }

    pub fn collect_cloned_bmap<F, M, N>(self, mapper: F) -> Result<BTreeMap<M, N>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, &'b [u8]) -> (M, N) + 'static,
        M: Ord + 'static,
        N: 'static,
    {
        self.map_static::<Result<(M, N), _>, _>(move |res| res.map(|(a, b)| mapper(a.as_reference(), &b))).collect()
    }

    pub fn collect_cloned_hashmap<F, M, N>(self, mapper: F) -> Result<HashMap<M, N>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, &'b [u8]) -> (M, N) + 'static,
        M: Hash + Eq + 'static,
        N: 'static,
    {
        self.map_static::<Result<(M, N), _>, _>(move |res| res.map(|(a, b)| mapper(a.as_reference(), &b))).collect()
    }

    pub fn collect_cloned_hashset<F, M>(self, mapper: F) -> Result<HashSet<M>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, &'b [u8]) -> M + 'static,
        M: Hash + Eq + 'static,
    {
        self.map_static::<Result<M, _>, _>(move |res| res.map(|(a, b)| mapper(a.as_reference(), &b))).collect()
    }

    pub fn first_cloned(
        mut self,
    ) -> Result<Option<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)>, Arc<SnapshotIteratorError>>
    {
        let item = self.next();
        item.transpose().map(|option| option.map(|(key, value)| (key.into_owned_array(), value.into_array())))
    }

    #[must_use]
    fn storage_next(
        storage: &mut MVCCRangeIterator,
    ) -> Option<Result<(StorageKey<'_, BUFFER_KEY_INLINE>, Bytes<'_, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>>
    {
        match storage.next()? {
            Ok((key, value)) => Some(Ok((StorageKey::Reference(key), Bytes::Reference(value)))),
            Err(error) => Some(Err(Arc::new(SnapshotIteratorError::MVCCRead { source: error }))),
        }
    }

    fn storage_peek(
        storage: &mut MVCCRangeIterator,
    ) -> Option<Result<(StorageKeyReference<'_>, &[u8]), Arc<SnapshotIteratorError>>> {
        match storage.peek()? {
            Ok(ok) => Some(Ok(ok)),
            Err(error) => Some(Err(Arc::new(SnapshotIteratorError::MVCCRead { source: error }))),
        }
    }

    /// Returns the next buffered write that is not a delete. In merged sources deletes are skipped by `resolve_merge`.
    #[must_use]
    fn buffered_next(
        buffered: &mut BufferRangeIterator,
    ) -> Option<Result<(StorageKey<'_, BUFFER_KEY_INLINE>, Bytes<'_, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>>
    {
        Self::skip_buffered_deletes(buffered);
        buffered.next().map(|(key, write)| Ok((StorageKey::Array(key), Bytes::Array(write.into_value()))))
    }

    fn buffered_peek(
        buffered: &mut BufferRangeIterator,
    ) -> Option<Result<(StorageKeyReference<'_>, &[u8]), Arc<SnapshotIteratorError>>> {
        Self::skip_buffered_deletes(buffered);
        buffered.peek().map(|(key, write)| Ok((StorageKeyReference::from(key), &**write.get_value())))
    }

    fn skip_buffered_deletes(buffered: &mut BufferRangeIterator) {
        while let Some((_, Write::Delete)) = buffered.peek() {
            buffered.next();
        }
    }
}

impl LendingIterator for SnapshotRangeIterator {
    type Item<'a> =
        Result<(StorageKey<'a, BUFFER_KEY_INLINE>, Bytes<'a, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.resolve_merge();
        match &mut self.sources {
            Sources::Empty => None,
            Sources::Storage(storage) => Self::storage_next(storage),
            Sources::Buffered(buffered) => Self::buffered_next(buffered),
            Sources::Merged { storage, buffered, ready_item_source } => {
                match ready_item_source.take().expect("resolved by resolve_merge") {
                    ReadyItemSource::Storage => Self::storage_next(storage),
                    ReadyItemSource::Buffered => Self::buffered_next(buffered),
                    ReadyItemSource::Both => {
                        // Skip the storage item and return the buffered one, because their values can differ
                        let _ = storage.next();
                        Self::buffered_next(buffered)
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ReadyItemSource {
    Storage,
    Buffered,
    Both,
}

#[derive(Debug)]
pub enum SnapshotIteratorError {
    MVCCRead { source: MVCCReadError },
}

impl fmt::Display for SnapshotIteratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for SnapshotIteratorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MVCCRead { source, .. } => Some(source),
        }
    }
}
