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

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use iterator::State;
use lending_iterator::LendingIterator;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};

use crate::{
    iterator::{MVCCRangeIterator, MVCCReadError},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::{buffer::BufferRangeIterator, write::Write},
};

pub struct SnapshotRangeIterator<'a, const PS: usize> {
    storage_iterator: MVCCRangeIterator<'a, PS>,
    buffered_iterator: Option<BufferRangeIterator>,
    iterator_state: IteratorState,
}

impl<'a, const PS: usize> SnapshotRangeIterator<'a, PS> {
    pub(crate) fn new(
        mvcc_iterator: MVCCRangeIterator<'a, PS>,
        buffered_iterator: Option<BufferRangeIterator>,
    ) -> Self {
        SnapshotRangeIterator {
            storage_iterator: mvcc_iterator,
            buffered_iterator,
            iterator_state: IteratorState::new(),
        }
    }

    pub fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        match self.iterator_state.state().clone() {
            State::Init => {
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => match self.iterator_state.source() {
                ReadyItemSource::Storage | ReadyItemSource::Both => {
                    Self::storage_peek(&mut self.storage_iterator).map(|some| some.map_err(Arc::new))
                }
                ReadyItemSource::Buffered => {
                    Some(Ok(Self::get_buffered_peek(self.buffered_iterator.as_mut().unwrap())))
                }
            },
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(error.clone())),
            State::Done => None,
        }
    }

    // a lending iterator trait is infeasible with the current borrow checker
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        match self.iterator_state.state() {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                let item = match self.iterator_state.source() {
                    ReadyItemSource::Storage | ReadyItemSource::Both => {
                        Self::storage_peek(&mut self.storage_iterator).map(|some| some.map_err(Arc::new))
                    }
                    ReadyItemSource::Buffered => {
                        Some(Ok(Self::get_buffered_peek(self.buffered_iterator.as_mut().unwrap())))
                    }
                };
                self.iterator_state.state = State::ItemUsed;
                item
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.next()
            }
            State::Error(error) => Some(Err(error.clone())),
            State::Done => None,
        }
    }

    pub fn seek(&mut self, key: StorageKeyReference<'_>) {
        match self.iterator_state.state() {
            State::Init | State::ItemUsed => {
                self.storage_iterator.seek(key.bytes());
                self.find_next_state()
            }
            State::ItemReady => {
                let (peek, _) = self.peek().unwrap().unwrap();
                if peek < key {
                    self.iterator_state.state = State::ItemUsed;
                    if let Some(buf) = self.buffered_iterator.as_mut() {
                        buf.seek(key.bytes())
                    }
                    self.storage_iterator.seek(key.bytes());
                    self.find_next_state()
                }
            }
            State::Error(_) | State::Done => {}
        }
    }

    fn find_next_state(&mut self) {
        assert!(
            matches!(self.iterator_state.state(), State::Init)
                || matches!(self.iterator_state.state(), State::ItemUsed)
        );
        while matches!(self.iterator_state.state(), State::Init)
            || matches!(self.iterator_state.state(), State::ItemUsed)
        {
            let mut advance_storage = false;
            let mut advance_buffered = false;
            let storage_peek = Self::storage_peek(&mut self.storage_iterator).transpose();
            let buffered_peek = Self::buffered_peek(&mut self.buffered_iterator);

            match (buffered_peek, storage_peek) {
                (None, Ok(None)) => {
                    self.iterator_state.state = State::Done;
                }
                (None, Ok(Some(_))) => self.iterator_state.set_item_ready(ReadyItemSource::Storage),
                (Some(Err(error)), _) | (_, Err(error)) => self.iterator_state.state = State::Error(Arc::new(error)),
                (Some(Ok((buffered_key, buffered_write))), Ok(storage_peek)) => {
                    (advance_storage, advance_buffered) =
                        Self::merge_buffered(&mut self.iterator_state, (buffered_key, buffered_write), storage_peek);
                }
            }
            if advance_storage {
                let _ = self.storage_iterator.next();
            }
            if advance_buffered {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
            }
        }
    }

    fn merge_buffered(
        iterator_state: &mut IteratorState,
        buffered_peek: (StorageKeyReference<'_>, &Write),
        storage_peek: Option<(StorageKeyReference<'_>, ByteReference<'_>)>,
    ) -> (bool, bool) {
        let (buffered_key, buffered_write) = buffered_peek;
        let mut advance_storage = false;
        let mut advance_buffered = false;
        let ordering = storage_peek.as_ref().map(|(storage_key, _)| buffered_key.cmp(storage_key));

        match ordering {
            None | Some(Ordering::Less) => {
                if buffered_write.is_delete() {
                    // SKIP buffered
                    advance_buffered = true;
                } else {
                    // ACCEPT buffered
                    iterator_state.set_item_ready(ReadyItemSource::Buffered);
                }
            }
            Some(Ordering::Equal) => {
                if buffered_write.is_delete() {
                    // SKIP both
                    advance_storage = true;
                    advance_buffered = true;
                } else {
                    debug_assert_eq!(storage_peek.unwrap().1.bytes(), buffered_write.get_value().bytes());
                    // ACCEPT both
                    iterator_state.set_item_ready(ReadyItemSource::Both);
                }
            }
            Some(Ordering::Greater) => iterator_state.set_item_ready(ReadyItemSource::Storage),
        }
        (advance_storage, advance_buffered)
    }

    fn buffered_peek(
        buffered_iterator: &mut Option<BufferRangeIterator>,
    ) -> Option<Result<(StorageKeyReference<'_>, &Write), SnapshotIteratorError>> {
        if let Some(buffered_iterator) = buffered_iterator {
            let buffered_peek = buffered_iterator.peek();
            match buffered_peek {
                None => None,
                Some(Ok((key, value))) => Some(Ok((StorageKeyReference::from(key), value))),
                Some(Err(error)) => Some(Err(error)),
            }
        } else {
            None
        }
    }

    fn storage_peek<'this>(
        storage_iterator: &'this mut MVCCRangeIterator<'_, PS>,
    ) -> Option<Result<(StorageKeyReference<'this>, ByteReference<'this>), SnapshotIteratorError>> {
        let storage_peek = storage_iterator.peek();
        match storage_peek {
            None => None,
            Some(Ok((key, value))) => Some(Ok((key, value))),
            Some(Err(error)) => Some(Err(SnapshotIteratorError::MVCCRead { source: error })),
        }
    }

    fn advance_and_find_next_state(&mut self) {
        assert!(matches!(self.iterator_state.state, State::ItemUsed));
        match self.iterator_state.source() {
            ReadyItemSource::Storage => {
                let _ = self.storage_iterator.next();
            }
            ReadyItemSource::Buffered => {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
            }
            ReadyItemSource::Both => {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
                let _ = self.storage_iterator.next();
            }
        }
        self.find_next_state();
    }

    fn get_buffered_peek(buffered_iterator: &mut BufferRangeIterator) -> (StorageKeyReference<'_>, ByteReference<'_>) {
        let (key, write) = buffered_iterator.peek().unwrap().unwrap();
        (StorageKeyReference::from(key), ByteReference::from(write.get_value()))
    }

    pub fn collect_cloned_vec<F, M>(mut self, mapper: F) -> Result<Vec<M>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, ByteReference<'b>) -> M,
    {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            match item {
                None => break,
                Some(Err(error)) => return Err(error),
                Some(Ok((key, value))) => {
                    vec.push(mapper(key, value));
                }
            }
        }
        Ok(vec)
    }

    pub fn collect_cloned_bmap<F, M, N>(mut self, mapper: F) -> Result<BTreeMap<M, N>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, ByteReference<'b>) -> (M, N),
        M: Ord + Eq + PartialEq,
    {
        let mut btree_map = BTreeMap::new();
        loop {
            let item = self.next();
            match item {
                None => break,
                Some(Err(error)) => return Err(error),
                Some(Ok((key, value))) => {
                    let (m, n) = mapper(key, value);
                    btree_map.insert(m, n);
                }
            }
        }
        Ok(btree_map)
    }

    pub fn collect_cloned_hashmap<F, M, N>(mut self, mapper: F) -> Result<HashMap<M, N>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, ByteReference<'b>) -> (M, N),
        M: Hash + Eq + PartialEq,
    {
        let mut map = HashMap::new();
        loop {
            let item = self.next();
            match item {
                None => break,
                Some(Err(error)) => return Err(error),
                Some(Ok((key, value))) => {
                    let (m, n) = mapper(key, value);
                    map.insert(m, n);
                }
            }
        }
        Ok(map)
    }

    pub fn collect_cloned_hashset<F, M>(mut self, mapper: F) -> Result<HashSet<M>, Arc<SnapshotIteratorError>>
    where
        F: for<'b> Fn(StorageKeyReference<'b>, ByteReference<'b>) -> M,
        M: Hash + Eq + PartialEq,
    {
        let mut set = HashSet::new();
        loop {
            let item = self.next();
            match item {
                None => break,
                Some(Err(error)) => return Err(error),
                Some(Ok((key, value))) => {
                    set.insert(mapper(key, value));
                }
            }
        }
        Ok(set)
    }

    pub fn first_cloned(
        mut self,
    ) -> Result<
        Option<(StorageKey<'static, BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)>,
        Arc<SnapshotIteratorError>,
    > {
        let item = self.next();
        item.transpose().map(|option| {
            option.map(|(key, value)| (StorageKey::Array(StorageKeyArray::from(key)), ByteArray::from(value)))
        })
    }

    pub fn count(mut self) -> usize {
        let mut count = 0;
        let mut next = self.next();
        while next.is_some() {
            next = self.next();
            count += 1;
        }
        count
    }
}

#[derive(Debug)]
struct IteratorState {
    state: State<Arc<SnapshotIteratorError>>,
    ready_item_source: ReadyItemSource,
}

impl IteratorState {
    fn new() -> IteratorState {
        IteratorState { state: State::Init, ready_item_source: ReadyItemSource::Storage }
    }

    fn state(&self) -> State<Arc<SnapshotIteratorError>> {
        self.state.clone()
    }

    fn source(&self) -> &ReadyItemSource {
        &self.ready_item_source
    }

    fn set_item_ready(&mut self, source: ReadyItemSource) {
        self.state = State::ItemReady;
        self.ready_item_source = source;
    }
}

#[derive(Debug)]
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
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SnapshotIteratorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MVCCRead { source, .. } => Some(source),
        }
    }
}
