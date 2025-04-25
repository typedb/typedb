/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, mem, mem::transmute};

use lending_iterator::{LendingIterator, Seekable};
use resource::profile::StorageCounters;
use rocksdb::DBRawIterator;

use crate::snapshot::pool::PoolRecycleGuard;

type KeyValue<'a> = (&'a [u8], &'a [u8]);

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
            IteratorItemState::None => {
                // unchanged
                Self::None
            }
            IteratorItemState::Some(_) => mem::replace(self, IteratorItemState::None),
            IteratorItemState::Finished => {
                // unchanged, keep finished
                Self::Finished
            }
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
                Ordering::Less => {
                    // fall through
                }
                Ordering::Equal => {
                    return;
                }
                Ordering::Greater => {
                    unreachable!("Cannot seek DBIterator to a value ordered behind the current item")
                }
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
