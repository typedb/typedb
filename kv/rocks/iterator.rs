/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{cmp::Ordering, mem, mem::transmute};

use bytes::{byte_array::ByteArray, Bytes};
use error::TypeDBError;
use lending_iterator::{LendingIterator, Seekable};
use primitive::key_range::{KeyRange, RangeEnd, RangeStart};
use resource::profile::StorageCounters;
use rocksdb::DBRawIterator;

use crate::{
    iterator::ContinueCondition,
    rocks::{pool::PoolRecycleGuard, RocksKVError, RocksKVStore},
};

type KeyValue<'a> = (&'a [u8], &'a [u8]);

pub struct RocksRangeIterator {
    iterator: DBIterator,
    continue_condition: ContinueCondition,
    is_finished: bool,
    kv_store_name: &'static str,
}

impl RocksRangeIterator {
    fn can_use_prefix<const INLINE_BYTES: usize>(
        prefix_length: Option<usize>,
        range: &KeyRange<Bytes<'_, INLINE_BYTES>>,
    ) -> bool {
        let Some(prefix_length) = prefix_length else {
            return false;
        };
        let start = range.start().get_value();
        if start.length() < prefix_length {
            return false;
        };
        match range.end() {
            RangeEnd::WithinStartAsPrefix => true,
            RangeEnd::EndPrefixInclusive(end) => {
                end.length() >= prefix_length && end[0..prefix_length] == start[0..prefix_length]
            }
            RangeEnd::EndPrefixExclusive(end) => {
                end.length() >= prefix_length && end[0..prefix_length] == start[0..prefix_length]
            }
            RangeEnd::Unbounded => false,
        }
    }

    fn may_skip_start(iterator: &mut DBIterator, excluded_value: &[u8]) {
        if iterator.peek().is_some_and(|result| result.is_ok_and(|(key, _)| key == excluded_value)) {
            iterator.next();
        }
    }

    fn accept_value(condition: &ContinueCondition, value: &<Self as LendingIterator>::Item<'_>) -> bool {
        match value {
            &Ok((key, _)) => {
                match condition {
                    ContinueCondition::ExactPrefix(prefix) => key.starts_with(prefix),
                    ContinueCondition::EndPrefixInclusive(end_inclusive) => {
                        // Either the key is before the end prefix in the dictionary order, OR starts with the end prefix.
                        // E.g. 'ABC' is sorted after 'AB', but will be included in the iterator output because it starts with AB.
                        key <= end_inclusive || key.starts_with(end_inclusive)
                    }
                    ContinueCondition::EndPrefixExclusive(end_exclusive) => key < end_exclusive,
                    ContinueCondition::Always => true,
                }
            }
            Err(_err) => true,
        }
    }

    pub(crate) fn new<const INLINE_BYTES: usize>(
        kv_store: &RocksKVStore,
        range: &KeyRange<Bytes<'_, INLINE_BYTES>>,
        storage_counters: StorageCounters,
    ) -> Self {
        let start_prefix = match range.start() {
            RangeStart::Inclusive(bytes) => Bytes::Reference(bytes.as_ref()),
            RangeStart::ExcludeFirstWithPrefix(bytes) => Bytes::Reference(bytes.as_ref()),
            RangeStart::ExcludePrefix(bytes) => {
                let mut cloned = bytes.to_array();
                cloned.increment().unwrap();
                Bytes::Array(cloned)
            }
        };
        let raw_iterator = if Self::can_use_prefix(kv_store.prefix_length, range) {
            kv_store.iterpool().get_iterator_prefixed(kv_store)
        } else {
            kv_store.iterpool().get_iterator_unprefixed(kv_store)
        };
        let mut iterator = DBIterator::new_from(raw_iterator, start_prefix.as_ref(), storage_counters);
        if matches!(range.start(), RangeStart::ExcludeFirstWithPrefix(_)) {
            Self::may_skip_start(&mut iterator, range.start().get_value());
        }

        let continue_condition = match range.end() {
            RangeEnd::WithinStartAsPrefix => {
                ContinueCondition::ExactPrefix(ByteArray::from(&**range.start().get_value()))
            }
            RangeEnd::EndPrefixInclusive(end) => ContinueCondition::EndPrefixInclusive(ByteArray::from(&**end)),
            RangeEnd::EndPrefixExclusive(end) => ContinueCondition::EndPrefixExclusive(ByteArray::from(&**end)),
            RangeEnd::Unbounded => ContinueCondition::Always,
        };
        Self { iterator, continue_condition, is_finished: false, kv_store_name: kv_store.name }
    }
}

impl LendingIterator for RocksRangeIterator {
    type Item<'a>
        = Result<(&'a [u8], &'a [u8]), Box<dyn TypeDBError>>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.is_finished {
            return None;
        }
        let next = self
            .iterator
            .next()
            .map(|result| result.map_err(|err| RocksKVError::Iterate { name: self.kv_store_name, source: err }.into()));

        // validate next against the Condition
        let item = match next {
            None => None,
            Some(result) => Self::accept_value(&self.continue_condition, &result).then_some(result),
        };
        if item.is_none() {
            self.is_finished = true;
        }
        item
    }
}

impl Seekable<[u8]> for RocksRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        if !self.is_finished {
            self.iterator.seek(key);
        }
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        compare_key(item, key)
    }
}

enum IteratorItemState {
    None,
    Some(KeyValue<'static>),
    Finished,
    Err(rocksdb::Error),
}

impl IteratorItemState {
    const fn is_none(&self) -> bool {
        matches!(self, IteratorItemState::None)
    }

    #[inline]
    pub fn take_value_else_retain(&mut self) -> Self {
        // this method protects us from losing the Finished or Error state
        match self {
            IteratorItemState::None => Self::None, // unchanged
            IteratorItemState::Some(_) => mem::replace(self, IteratorItemState::None),
            IteratorItemState::Finished => Self::Finished, // unchanged, keep finished
            IteratorItemState::Err(err) => Self::Err(err.clone()),
        }
    }
}

/// SAFETY NOTE: `'static` here represents that the `DBIterator` owns the data.
/// The item's lifetime is in fact invalidated when `iterator` is advanced.
pub(super) struct DBIterator {
    iterator: PoolRecycleGuard<DBRawIterator<'static>>,
    storage_counters: StorageCounters,
    // NOTE: when item is empty, that means that the kv pair the Rocks iterator _is currently pointing to_
    //       has been yielded to the user, and the underlying iterator needs to be advanced before  reading
    state: IteratorItemState,
}

impl DBIterator {
    pub(super) fn new_from(
        mut iterator: PoolRecycleGuard<DBRawIterator<'static>>,
        start: &[u8],
        storage_counters: StorageCounters,
    ) -> Self {
        iterator.seek(start);
        storage_counters.increment_raw_seek();
        let mut this = Self { iterator, state: IteratorItemState::None, storage_counters };
        this.record_iterator_state(); // initialise with the first state read from the seek'ed value
        this
    }

    pub(super) fn peek(&mut self) -> Option<<Self as LendingIterator>::Item<'_>> {
        let state = self.next_internal();
        self.state = state;
        match &self.state {
            IteratorItemState::None => unreachable!("State after internal check should be error, Some, or Finished"),
            IteratorItemState::Some(kv) => Some(Ok(*kv)),
            IteratorItemState::Finished => None,
            IteratorItemState::Err(err) => Some(Err(err.clone())),
        }
    }

    fn next_internal(&mut self) -> IteratorItemState {
        if !self.state.is_none() {
            self.state.take_value_else_retain()
        } else {
            self.storage_counters.increment_raw_advance();
            self.iterator.next();
            self.record_iterator_state();
            self.state.take_value_else_retain()
        }
    }

    fn record_iterator_state(&mut self) {
        self.state = match self.iterator.item() {
            None => match self.iterator.status() {
                Ok(_) => IteratorItemState::Finished,
                Err(err) => IteratorItemState::Err(err),
            },
            Some(item) => {
                let kv = unsafe { transmute::<KeyValue<'_>, KeyValue<'static>>(item) };
                IteratorItemState::Some(kv)
            }
        }
    }
}

impl LendingIterator for DBIterator {
    type Item<'a>
        = Result<(&'a [u8], &'a [u8]), rocksdb::Error>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let next_state = self.next_internal();
        match next_state {
            IteratorItemState::None => unreachable!("State after internal check should be error, Some, or Finished"),
            IteratorItemState::Some(kv) => Some(Ok(kv)),
            IteratorItemState::Finished => None,
            IteratorItemState::Err(err) => Some(Err(err)),
        }
    }
}

impl Seekable<[u8]> for DBIterator {
    fn seek(&mut self, key: &[u8]) {
        if matches!(&self.state, IteratorItemState::Finished) {
            return;
        } else if let IteratorItemState::Some((item_key, _)) = &self.state {
            match (*item_key).cmp(key) {
                Ordering::Less => (), // fall through
                Ordering::Equal => return,
                Ordering::Greater => unreachable!("Cannot seek DBIterator to a value ordered behind the current item"),
            }
        }
        self.state.take_value_else_retain();
        self.iterator.seek(key);
        self.storage_counters.increment_raw_seek();
        self.record_iterator_state()
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        compare_key(item, key)
    }
}

pub(super) fn compare_key<E>(item: &Result<(&[u8], &[u8]), E>, key: &[u8]) -> Ordering {
    if let Ok(item) = item {
        let (peek, _) = item;
        peek.cmp(&key)
    } else {
        Ordering::Equal
    }
}
