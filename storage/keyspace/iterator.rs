use std::cmp::Ordering;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use iterator::State;
use logger::result::ResultExt;
use primitive::prefix_range::PrefixRange;
use speedb::{self, DBRawIterator, DBRawIteratorWithThreadMode, DB};

use super::keyspace::{Keyspace, KeyspaceError, KeyspaceErrorKind};

pub struct KeyspaceRangeIterator<'a, const INLINE_BYTES: usize> {
    pub(crate) range: PrefixRange<Bytes<'a, { INLINE_BYTES }>>,
    pub(crate) iterator: DBRawIterator<'a>,
    pub(crate) state: State<speedb::Error>,
}

impl<'a, const INLINE_BYTES: usize> KeyspaceRangeIterator<'a, INLINE_BYTES> {
    pub(crate) fn new(keyspace: &'a Keyspace, range: PrefixRange<Bytes<'a, { INLINE_BYTES }>>) -> Self {
        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);
        let read_opts = keyspace.new_read_options();
        let raw_iterator: DBRawIteratorWithThreadMode<'a, DB> = keyspace.kv_storage.raw_iterator_opt(read_opts);

        KeyspaceRangeIterator { range, iterator: raw_iterator, state: State::Init }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match &self.state {
            State::Init => {
                self.iterator.seek(self.range.start().bytes());
                self.update_state();
                self.peek()
            }
            State::ItemReady => {
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.peek()
            }
            State::Error(error) => {
                Some(Err(KeyspaceError { kind: KeyspaceErrorKind::Iterate { source: error.clone() } }))
            }
            State::Done => None,
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match &self.state {
            State::Init => {
                self.iterator.seek(self.range.start().bytes());
                self.update_state();
                self.next()
            }
            State::ItemReady => {
                self.state = State::ItemUsed;
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.next()
            }
            State::Error(error) => {
                Some(Err(KeyspaceError { kind: KeyspaceErrorKind::Iterate { source: error.clone() } }))
            }
            State::Done => None,
        }
    }

    pub(crate) fn seek(&mut self, key: &[u8]) {
        match &self.state {
            State::Done | State::Error(_) => {}
            State::Init => {
                if self.is_in_range(key) {
                    self.iterator.seek(key);
                    self.update_state();
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemReady => {
                let valid_prefix = self.is_in_range(key);
                if valid_prefix {
                    match self.peek().unwrap().unwrap().0.cmp(key) {
                        Ordering::Less => {
                            self.iterator.seek(key);
                            self.update_state();
                        }
                        Ordering::Equal => {}
                        Ordering::Greater => unreachable!("Cannot seek backward."),
                    }
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.seek(key)
            }
        }
    }

    pub(crate) fn advance_and_update_state(&mut self) {
        assert!(matches!(self.state, State::ItemUsed));
        self.iterator.next();
        self.update_state();
    }

    pub(crate) fn update_state(&mut self) {
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

    pub(crate) fn is_in_range(&self, key: &[u8]) -> bool {
        self.range.contains(Bytes::Reference(ByteReference::new(key)))
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
