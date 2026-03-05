/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::byte_array::ByteArray;
use error::TypeDBError;
use lending_iterator::{LendingIterator, Seekable};
use resource::constants::kv::ITERATOR_CONTINUE_CONDITION_INLINE;

use crate::rocks::iterator::RocksRangeIterator;

pub type KVIteratorItem<'a> = Result<(&'a [u8], &'a [u8]), Box<dyn TypeDBError>>;

pub enum KVRangeIterator {
    RocksDB(RocksRangeIterator),
}

impl LendingIterator for KVRangeIterator {
    type Item<'a> = KVIteratorItem<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            Self::RocksDB(iter) => iter.next(),
        }
    }
}

impl Seekable<[u8]> for KVRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        match self {
            Self::RocksDB(iter) => iter.seek(key),
        }
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        match self {
            Self::RocksDB(iter) => iter.compare_key(item, key),
        }
    }
}

pub(crate) enum ContinueCondition {
    ExactPrefix(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    EndPrefixInclusive(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    EndPrefixExclusive(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    Always,
}
