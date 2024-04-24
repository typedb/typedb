/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use iterator::State;
use logger::result::ResultExt;
use speedb::{self, DBRawIterator, DBRawIteratorWithThreadMode, DB};
use crate::key_range::KeyRange;

use super::keyspace::{Keyspace, KeyspaceError};

pub struct KeyspaceRangeIterator<'a, const INLINE_BYTES: usize> {
    range: KeyRange<Bytes<'a, { INLINE_BYTES }>>,
    iterator: DBRawIterator<'a>,
    state: State<speedb::Error>,
}

impl<'a, const INLINE_BYTES: usize> KeyspaceRangeIterator<'a, INLINE_BYTES> {
    pub(crate) fn new(keyspace: &'a Keyspace, range: KeyRange<Bytes<'a, { INLINE_BYTES }>>) -> Self {
        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);
        let read_opts = keyspace.new_read_options();
        let raw_iterator: DBRawIteratorWithThreadMode<'a, DB> = keyspace.kv_storage.raw_iterator_opt(read_opts);

        KeyspaceRangeIterator { range, iterator: raw_iterator, state: State::Init }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match self.state.clone() {
            State::Init => {
                self.iterator.seek(self.range.start().bytes());
                self.update_state();
                self.peek()
            }
            State::ItemUsed => {
                self.iterator.next();
                self.update_state();
                self.peek()
            }
            State::ItemReady => {
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::Error(error) => Some(Err(KeyspaceError::Iterate { source: error.clone() })),
            State::Done => None,
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match self.state.clone() {
            State::Init => {
                self.iterator.seek(self.range.start().bytes());
                self.update_state();
                self.next()
            }
            State::ItemUsed => {
                self.iterator.next();
                self.update_state();
                self.next()
            }
            State::ItemReady => {
                self.state = State::ItemUsed;
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::Error(error) => Some(Err(KeyspaceError::Iterate { source: error.clone() })),
            State::Done => None,
        }
    }

    pub(crate) fn seek(&mut self, key: &[u8]) {
        match self.state {
            State::Done | State::Error(_) => {}
            State::Init => {
                if self.is_in_range(key) {
                    // TODO is this right?
                    self.iterator.seek(key);
                    self.update_state();
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemReady => {
                if self.is_in_range(key) {
                    let (peek, _) = self.peek().unwrap().unwrap();
                    match peek.cmp(key) {
                        Ordering::Less => {
                            self.state = State::ItemUsed;
                            self.iterator.seek(key);
                            self.update_state();
                        }
                        Ordering::Equal => {}
                        Ordering::Greater => {
                            // TODO: seeking backward could be a no-op or an error or illegal state??
                        }
                    }
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemUsed => {
                let prev = self.iterator.key().unwrap();
                match prev.cmp(key) {
                    Ordering::Less => {
                        self.iterator.seek(key);
                        self.update_state();
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        // TODO: seeking backward could be a no-op or an error or illegal state??
                    }
                }
            }
        }
    }

    fn update_state(&mut self) {
        if self.iterator.valid() {
            if self.is_in_range(self.iterator.key().unwrap()) {
                self.state = State::ItemReady;
            } else {
                self.state = State::Done;
            }
        } else if self.iterator.status().is_err() {
            self.state = State::Error(self.iterator.status().err().unwrap().clone());
        } else {
            self.state = State::Done;
        }
    }

    ///
    /// Optimise range-check. We only need to do a single comparison, to the end
    /// of the range, since we can guarantee that we always start within the range and move forward.
    fn is_in_range(&self, key: &[u8]) -> bool {
        self.range.within_end(Bytes::Reference(ByteReference::new(key)))
    }

    pub fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(
        mut self,
    ) -> Vec<(ByteArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            if item.is_none() {
                break;
            }
            let (key, value) = item.unwrap().unwrap_or_log();
            vec.push((ByteArray::<INLINE_KEY>::copy(key), ByteArray::<INLINE_VALUE>::copy(value)));
        }
        vec
    }
}
