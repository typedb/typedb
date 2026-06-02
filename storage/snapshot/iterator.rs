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
    storage_iterator: Option<MVCCRangeIterator>,
    buffered_iterator: Option<BufferRangeIterator>,
    ready_item_source: Option<ReadyItemSource>,
}

impl SnapshotRangeIterator {
    pub(crate) fn new(mvcc_iterator: MVCCRangeIterator, buffered_iterator: Option<BufferRangeIterator>) -> Self {
        SnapshotRangeIterator { storage_iterator: Some(mvcc_iterator), buffered_iterator, ready_item_source: None }
    }
    pub fn new_buffered_only(buffered_iterator: BufferRangeIterator) -> Self {
        SnapshotRangeIterator {
            storage_iterator: None,
            buffered_iterator: Some(buffered_iterator),
            ready_item_source: None,
        }
    }

    // for testing
    pub fn new_empty() -> Self {
        SnapshotRangeIterator { storage_iterator: None, buffered_iterator: None, ready_item_source: None }
    }

    pub fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), Arc<SnapshotIteratorError>>> {
        if self.ready_item_source.is_none() {
            self.find_next_state();
        }

        match self.ready_item_source? {
            ReadyItemSource::Storage => match self.storage_iterator.as_mut().unwrap().peek()? {
                &Ok(ok) => Some(Ok(ok)),
                Err(error) => Some(Err(Arc::new(SnapshotIteratorError::MVCCRead { source: error.clone() }))),
            },
            ReadyItemSource::Buffered | ReadyItemSource::Both => self.buffered_peek(),
        }
    }

    pub fn seek(&mut self, key: StorageKeyReference<'_>) {
        if let Some(Ok((peek, _))) = self.peek() {
            if peek < key {
                self.ready_item_source = None;
                if let Some(iter) = self.buffered_iterator.as_mut() {
                    // buffered iterators check that the seek is in ascending order internally
                    iter.seek(key.bytes())
                }
                if let Some(iter) = self.storage_iterator.as_mut() {
                    // storage iterators check that the seek is in ascending order internally
                    iter.seek(key.bytes());
                }
                self.find_next_state();
            }
        }
    }

    fn find_next_state(&mut self) {
        while self.ready_item_source.is_none() {
            // get the storage key if there is one, else just fall back to the next buffered key
            let storage_peek = self.storage_iterator.as_mut().and_then(|iter| iter.peek());
            let Some(Ok((storage_key, _storage_value))) = storage_peek else {
                if let Some(buffered_iterator) = self.buffered_iterator.as_mut() {
                    while let Some((_, Write::Delete)) = buffered_iterator.peek() {
                        buffered_iterator.next();
                    }
                    if buffered_iterator.peek().is_some() {
                        self.ready_item_source = Some(ReadyItemSource::Buffered);
                    }
                }
                break;
            };

            // if we have a storage key, try to get the next buffered key, else just fall back to storage key
            let Some(Some((buffered_key, buffered_write))) = self.buffered_iterator.as_mut().map(|iter| iter.peek())
            else {
                self.ready_item_source = Some(ReadyItemSource::Storage);
                break;
            };

            // if we have both, compare
            let (buffered_key, buffered_write) = (StorageKeyReference::from(buffered_key), buffered_write);
            match buffered_key.cmp(storage_key) {
                Ordering::Less => {
                    if buffered_write.is_delete() {
                        // SKIP buffered
                        self.buffered_iterator.as_mut().map(|iter| iter.next());
                    } else {
                        // ACCEPT buffered
                        assert!(self.buffered_iterator.is_some());
                        self.ready_item_source = Some(ReadyItemSource::Buffered);
                    }
                }
                Ordering::Equal => {
                    if buffered_write.is_delete() {
                        // SKIP both
                        self.storage_iterator.as_mut().unwrap().next();
                        self.buffered_iterator.as_mut().map(|iter| iter.next());
                    } else {
                        // ACCEPT both
                        assert!(self.buffered_iterator.is_some());
                        self.ready_item_source = Some(ReadyItemSource::Both);
                    }
                }
                Ordering::Greater => {
                    self.ready_item_source = Some(ReadyItemSource::Storage);
                }
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
        &mut self,
    ) -> Option<Result<(StorageKey<'_, BUFFER_KEY_INLINE>, Bytes<'_, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>>
    {
        match self.storage_iterator.as_mut().unwrap().next()? {
            Ok((key, value)) => Some(Ok((StorageKey::Reference(key), Bytes::Reference(value)))),
            Err(error) => Some(Err(Arc::new(SnapshotIteratorError::MVCCRead { source: error.clone() }))),
        }
    }

    fn storage_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), SnapshotIteratorError>> {
        match self.storage_iterator.as_mut().unwrap().peek()? {
            &Ok((key, value)) => Some(Ok((key, value))),
            Err(error) => Some(Err(SnapshotIteratorError::MVCCRead { source: error.clone() })),
        }
    }

    #[must_use]
    fn buffered_next(
        &mut self,
    ) -> Option<Result<(StorageKey<'_, BUFFER_KEY_INLINE>, Bytes<'_, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>>
    {
        assert!(self.buffered_iterator.is_some());
        self.buffered_iterator.as_mut().unwrap().next().map(|(key, write)| {
            assert!(!write.is_delete());
            Ok((StorageKey::Array(key), Bytes::Array(write.into_value())))
        })
    }

    fn buffered_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), Arc<SnapshotIteratorError>>> {
        assert!(self.buffered_iterator.is_some());
        self.buffered_iterator
            .as_mut()
            .unwrap()
            .peek()
            .map(|(key, write)| Ok((StorageKeyReference::from(key), &**write.get_value())))
    }
}

impl LendingIterator for SnapshotRangeIterator {
    type Item<'a> =
        Result<(StorageKey<'a, BUFFER_KEY_INLINE>, Bytes<'a, BUFFER_VALUE_INLINE>), Arc<SnapshotIteratorError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.ready_item_source.is_none() {
            self.find_next_state();
        }
        let next = match self.ready_item_source.take() {
            Some(ReadyItemSource::Both) => {
                // Skip the storage and get the buffered value because they can be different
                let _ = self.storage_next();
                self.buffered_next()
            }
            Some(ReadyItemSource::Storage) => self.storage_next(),
            Some(ReadyItemSource::Buffered) => self.buffered_next(),
            None => None,
        };
        next
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
