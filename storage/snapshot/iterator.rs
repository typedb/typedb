/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::cmp::Ordering;
use std::sync::Arc;
use bytes::byte_array::ByteArray;

use iterator::State;

use crate::key_value::{StorageKey, StorageKeyArray, StorageKeyReference, StorageValue, StorageValueArray, StorageValueReference};
use crate::MVCCPrefixIterator;
use crate::snapshot::buffer::{BUFFER_INLINE_KEY, BUFFER_INLINE_VALUE, BufferedPrefixIterator};
use crate::snapshot::snapshot::{SnapshotError, SnapshotErrorKind};
use crate::snapshot::write::Write;

pub struct SnapshotPrefixIterator<'a> {
    storage_iterator: MVCCPrefixIterator<'a>,
    buffered_iterator: Option<BufferedPrefixIterator>,
    iterator_state: IteratorState,
}

#[derive(Debug)]
struct IteratorState {
    state: State<Arc<SnapshotError>>,
    ready_item_source: ReadyItemSource,
}

impl IteratorState {
    fn new() -> IteratorState {
        IteratorState {
            state: State::Init,
            ready_item_source: ReadyItemSource::Storage,
        }
    }

    fn state(&self) -> &State<Arc<SnapshotError>> {
        &self.state
    }

    fn source(&self) -> &ReadyItemSource {
        &self.ready_item_source
    }

    fn set_init(&mut self) {
        self.state = State::Init;
    }

    fn set_item_ready(&mut self, source: ReadyItemSource) {
        self.state = State::ItemReady;
        self.ready_item_source = source;
    }

    fn set_item_used(&mut self) {
        self.state = State::ItemUsed;
    }

    fn set_done(&mut self) {
        self.state = State::Done;
    }

    fn set_error(&mut self, error: Arc<SnapshotError>) {
        self.state = State::Error(error)
    }
}

#[derive(Debug)]
enum ReadyItemSource {
    Storage,
    Buffered,
    Both,
}

impl<'a> SnapshotPrefixIterator<'a> {
    pub(crate) fn new(mvcc_iterator: MVCCPrefixIterator<'a>, buffered_iterator: Option<BufferedPrefixIterator>) -> SnapshotPrefixIterator<'a> {
        SnapshotPrefixIterator {
            storage_iterator: mvcc_iterator,
            buffered_iterator: buffered_iterator,
            iterator_state: IteratorState::new(),
        }
    }

    pub fn peek<'this>(&'this mut self) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>), SnapshotError>> {
        match self.iterator_state.state().clone() {
            State::Init => {
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => {
                match self.iterator_state.source().clone() {
                    ReadyItemSource::Storage | ReadyItemSource::Both => Self::storage_peek(&mut self.storage_iterator),
                    ReadyItemSource::Buffered => Some(Ok(Self::get_buffered_peek(self.buffered_iterator.as_mut().unwrap()))),
                }
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(SnapshotError {
                kind: SnapshotErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None,
        }
    }

    pub fn next(&mut self) -> Option<Result<(StorageKey<BUFFER_INLINE_KEY>, StorageValue<BUFFER_INLINE_VALUE>), SnapshotError>> {
        match self.iterator_state.state().clone() {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                let item = match self.iterator_state.source().clone() {
                    ReadyItemSource::Storage | ReadyItemSource::Both => Self::storage_peek(&mut self.storage_iterator),
                    ReadyItemSource::Buffered => Some(Ok(Self::get_buffered_peek(self.buffered_iterator.as_mut().unwrap()))),
                };
                self.iterator_state.set_item_used();
                item
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.next()
            }
            State::Error(error) => Some(Err(SnapshotError {
                kind: SnapshotErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None
        }
    }

    pub fn seek(&mut self) {
        // if state == DONE | ERROR -> do nothing
        // if state == INIT -> seek(), state = UPDATING, update_state()
        // if state == EMPTY -> seek(), state = UPDATING, update_state() TODO: compare to previous to prevent backward seek?
        // if state == READY -> seek(), state = UPDATING, update_state() TODO: compare to peek() to prevent backwrard seek?

        todo!()
    }

    fn find_next_state(&mut self) {
        assert!(matches!(self.iterator_state.state(), &State::Init) || matches!(self.iterator_state.state(), &State::ItemUsed));
        while matches!(self.iterator_state.state(), &State::Init) || matches!(self.iterator_state.state(), &State::ItemUsed) {
            let mut advance_storage = false;
            let mut advance_buffered = false;
            let storage_peek = Self::storage_peek(&mut self.storage_iterator).transpose();
            let buffered_peek = Self::buffered_peek(&mut self.buffered_iterator);

            match (buffered_peek, storage_peek) {
                (None, Ok(None)) => self.iterator_state.set_done(),
                (None, Ok(Some(_))) => self.iterator_state.set_item_ready(ReadyItemSource::Storage),
                (Some(Err(error)), _) | (_, Err(error)) => self.iterator_state.set_error(Arc::new(error)),
                (Some(Ok((buffered_key, buffered_write))), Ok(storage_peek)) => {
                    (advance_storage, advance_buffered) = Self::merge_buffered(&mut self.iterator_state, (buffered_key, buffered_write), storage_peek);
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

    fn merge_buffered(iterator_state: &mut IteratorState, buffered_peek: (StorageKey<BUFFER_INLINE_KEY>, &Write), storage_peek: Option<(StorageKey<BUFFER_INLINE_KEY>, StorageValue<BUFFER_INLINE_VALUE>)>) -> (bool, bool) {
        let (buffered_key, buffered_write) = buffered_peek;
        let mut advance_storage = false;
        let mut advance_buffered = false;
        let ordering = storage_peek.as_ref().map(|(storage_key, _)| buffered_key.cmp(storage_key));

        match ordering {
            None | Some(Ordering::Less) => if buffered_write.is_delete() {
                // SKIP buffered
                advance_buffered = true;
            } else {
                // ACCEPT buffered
                iterator_state.set_item_ready(ReadyItemSource::Buffered);
            },
            Some(Ordering::Equal) => if buffered_write.is_delete() {
                // SKIP both
                advance_storage = true;
                advance_buffered = true;
            } else {
                debug_assert_eq!(storage_peek.unwrap().1.bytes(), buffered_write.get_value().bytes());
                // ACCEPT both
                iterator_state.set_item_ready(ReadyItemSource::Both);
            },
            Some(Ordering::Greater) => iterator_state.set_item_ready(ReadyItemSource::Storage),
        }
        (advance_storage, advance_buffered)
    }

    fn buffered_peek<'this>(buffered_iterator: &'this mut Option<BufferedPrefixIterator>) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, &Write), SnapshotError>> {
        if let Some(buffered_iterator) = buffered_iterator {
            let buffered_peek = buffered_iterator.peek();
            match buffered_peek {
                None => None,
                Some(Ok((key, value))) => {
                    Some(Ok((StorageKey::Reference(StorageKeyReference::from(key)), value)))
                }
                Some(Err(error)) => Some(Err(error)),
            }
        } else {
            None
        }
    }

    fn storage_peek<'this>(storage_iterator: &'this mut MVCCPrefixIterator) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>), SnapshotError>> {
        let storage_peek = storage_iterator.peek();
        match storage_peek {
            None => None,
            Some(Ok((key, value))) => {
                Some(Ok((StorageKey::Reference(key), StorageValue::Reference(value))))
            }
            Some(Err(error)) => {
                Some(Err(SnapshotError {
                    kind: SnapshotErrorKind::FailedMVCCStorageIterate {
                        source: error
                    }
                }))
            }
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

    fn get_buffered_peek<'this>(buffered_iterator: &'this mut BufferedPrefixIterator) -> (StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>) {
        let (key, write) = buffered_iterator.peek().unwrap().unwrap();
        (
            StorageKey::Reference(StorageKeyReference::from(key)),
            StorageValue::Reference(StorageValueReference::from(write.get_value()))
        )
    }

    pub fn collect_cloned<'t>(mut self) -> Vec<(StorageKey<'t, BUFFER_INLINE_KEY>, StorageValue<'t, BUFFER_INLINE_VALUE>)> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            if item.is_none() {
                break;
            }
            let (key, value) = item.unwrap().unwrap();

            vec.push((
                StorageKey::Array(StorageKeyArray::new(key.keyspace_id(), ByteArray::from(key.bytes()))),
                StorageValue::Array(StorageValueArray::new(ByteArray::from(value.bytes())))
             ));
        }
        vec
    }
}


// storage_iterator.merge_join_by(
//     buffered_iterator,
//     |(k1, v1), (k2, v2)| k1.cmp(k2),
// ).filter_map(|ordering| match ordering {
//     EitherOrBoth::Both(Ok((k1, v1)), (k2, write2)) => match write2 {
//         Write::Insert(v2) => Some((k2, v2)),
//         Write::InsertPreexisting(v2, _) => Some((k2, v2)),
//         Write::RequireExists(v2) => {
//             debug_assert_eq!(v1, v2);
//             Some((k1, v1))
//         }
//         Write::Delete => None,
//     },
//     EitherOrBoth::Left(Ok((k1, v1))) => Some((k1, v1)),
//     EitherOrBoth::Right((k2, write2)) => match write2 {
//         Write::Insert(v2) => Some((k2, v2)),
//         Write::InsertPreexisting(v2, _) => Some((k2, v2)),
//         Write::RequireExists(_) => unreachable!("Invalid state: a key required to exist must also exists in Storage."),
//         Write::Delete => None,
//     },
//     EitherOrBoth::Both(Err(_), _) => {
//         panic!("Unhandled error in iteration")
//     },
//     EitherOrBoth::Left(Err(_)) => {
//         panic!("Unhandled error in iteration")
//     },
// })

// TODO replace

//     .map(|result| result.map(|(k, v)| {
//     (StorageKey::Reference(k), StorageValue::Reference(v))
// }))
