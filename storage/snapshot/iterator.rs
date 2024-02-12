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

use iterator::State;

use crate::key_value::{StorageKey, StorageKeyReference, StorageValue, StorageValueReference};
use crate::MVCCPrefixIterator;
use crate::snapshot::buffer::{BUFFER_INLINE_KEY, BUFFER_INLINE_VALUE, BufferedPrefixIterator};
use crate::snapshot::snapshot::{SnapshotError, SnapshotErrorKind};
use crate::snapshot::write::Write;

struct SnapshotPrefixIterator<'a> {
    storage_iterator: MVCCPrefixIterator<'a>,
    buffered_iterator: Option<BufferedPrefixIterator>,
    state: State<Arc<SnapshotError>>,
    item_source: ItemSource,
}

enum ItemSource {
    Storage,
    Buffered,
    Both,
}

impl<'a> SnapshotPrefixIterator<'a> {
    fn new(&self, mvcc_iterator: MVCCPrefixIterator<'a>, buffered_iterator: Option<BufferedPrefixIterator>) -> SnapshotPrefixIterator<'a> {
        SnapshotPrefixIterator {
            storage_iterator: mvcc_iterator,
            buffered_iterator: buffered_iterator,
            state: State::Unknown,
            item_source: ItemSource::Storage,
        }
    }

    fn peek<'this>(&'this mut self) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>), SnapshotError>> {
        match &self.state {
            State::Unknown => {
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => {
                match self.item_source {
                    ItemSource::Storage | ItemSource::Both => self.storage_peek(),
                    ItemSource::Buffered => Some(Ok(self.get_buffered_peek())),
                }
            }
            State::ItemUsed => {
                self.consume_used_item();
                self.find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(SnapshotError {
                kind: SnapshotErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None,
        }
    }

    fn next(&mut self) -> Option<Result<(StorageKey<BUFFER_INLINE_KEY>, StorageValue<BUFFER_INLINE_VALUE>), SnapshotError>> {
        match &self.state {
            State::Unknown => {
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                self.state = State::ItemUsed;
                let item = match self.item_source {
                    ItemSource::Storage | ItemSource::Both => self.storage_peek(),
                    ItemSource::Buffered => Some(Ok(self.get_buffered_peek())),
                };
                item
            }
            State::ItemUsed => {
                self.consume_used_item();
                self.find_next_state();
                self.next()
            }
            State::Error(error) => Some(Err(SnapshotError {
                kind: SnapshotErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None
        }
    }

    fn find_next_state(&mut self) {
        assert_eq!(self.state, State::Unknown);
        while self.state == State::Unknown {
            let mut advance_storage = false;
            let mut advance_buffered = false;
            match self.buffered_peek() {
                None => {
                    match self.storage_peek() {
                        None => self.state = State::Done,
                        Some(Ok(_)) => {
                            self.state = State::ItemReady;
                            self.item_source = ItemSource::Storage;
                        }
                        Some(Err(error)) => self.state = State::Error(Arc::new(error)),
                    }
                }
                Some(Ok((buffered_key, buffered_write))) => {
                    match self.storage_peek() {
                        None => {
                            if buffered_write.is_delete() {
                                // SKIP buffered
                                advance_buffered = true;
                            } else {
                                // ACCEPT buffered
                                self.state = State::ItemReady;
                                self.item_source = ItemSource::Buffered;
                            }
                        }
                        Some(Ok((storage_key, storage_value))) => {
                            let cmp = buffered_key.cmp(&storage_key);
                            match cmp {
                                Ordering::Less => {
                                    if buffered_write.is_delete() {
                                        // SKIP buffered
                                        advance_buffered = true;
                                    } else {
                                        self.state = State::ItemReady;
                                        self.item_source = ItemSource::Buffered;
                                    }
                                }
                                Ordering::Equal => {
                                    if buffered_write.is_delete() {
                                        // SKIP both
                                        advance_storage = true;
                                        advance_buffered = true;
                                    } else {
                                        debug_assert_eq!(storage_value.bytes(), buffered_write.get_value().bytes());
                                        // ACCEPT both
                                        self.state = State::ItemReady;
                                        self.item_source = ItemSource::Both;
                                    }
                                }
                                Ordering::Greater => {
                                    self.state = State::ItemReady;
                                    self.item_source = ItemSource::Storage;
                                }
                            }
                        }
                        Some(Err(error)) => self.state = State::Error(Arc::new(error)),
                    }
                }
                Some(Err(error)) => self.state = State::Error(Arc::new(error)),
            }

            if advance_buffered {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
            }
            if advance_storage {
                let _ = self.storage_iterator.next();
            }
        }
    }

    fn merge_buffered(&mut self, buffered_peek: (StorageKey<BUFFER_INLINE_KEY>, &Write), storage_peek: Option<Result<(StorageKey<BUFFER_INLINE_KEY>, StorageValue<BUFFER_INLINE_VALUE>), SnapshotError>>) -> (bool, bool) {
        let (buffered_key, buffered_write) = buffered_peek;
        let mut advance_storage = false;
        let mut advance_buffered = false;

        (advance_storage, advance_buffered)
    }

    fn buffered_peek<'this>(&'this mut self) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, &Write), SnapshotError>> {
        if let Some(buffered_iterator) = &mut self.buffered_iterator {
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

    fn storage_peek<'this>(&'this mut self) -> Option<Result<(StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>), SnapshotError>> {
        let storage_peek = self.storage_iterator.peek();
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

    fn consume_used_item(&mut self) {
        assert_eq!(self.state, State::ItemUsed);
        match self.item_source {
            ItemSource::Storage => {
                let _ = self.storage_iterator.next();
            }
            ItemSource::Buffered => {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
            }
            ItemSource::Both => {
                let _ = self.buffered_iterator.as_mut().unwrap().next();
                let _ = self.storage_iterator.next();
            }
        }
        self.state = State::Unknown;
    }

    fn get_buffered_peek<'this>(&'this mut self) -> (StorageKey<'this, BUFFER_INLINE_KEY>, StorageValue<'this, BUFFER_INLINE_VALUE>) {
        let buffered_iterator = self.buffered_iterator.as_mut().unwrap();
        let (key, write) = buffered_iterator.peek().unwrap().unwrap();
        (
            StorageKey::Reference(StorageKeyReference::from(key)),
            StorageValue::Reference(StorageValueReference::from(write.get_value()))
        )
    }

    fn seek(&mut self) {
        // if state == DONE | ERROR -> do nothing
        // if state == INIT -> seek(), state = UPDATING, update_state()
        // if state == EMPTY -> seek(), state = UPDATING, update_state() TODO: compare to previous to prevent backward seek?
        // if state == READY -> seek(), state = UPDATING, update_state() TODO: compare to peek() to prevent backwrard seek?

        todo!()
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
