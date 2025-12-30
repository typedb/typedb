/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, Bytes};
use lending_iterator::{LendingIterator, Seekable};
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;

use crate::{rocks::pool::Poolable, KVStore, KVStoreError};

pub type KVIteratorItem<'a> = Result<(&'a [u8], &'a [u8]), Box<dyn KVStoreError>>;

pub trait KVStoreRangeIterator: for<'a> LendingIterator<Item<'a> = KVIteratorItem<'a>> + Seekable<[u8]> {
    type KVStore: KVStore;

    fn new<'a, const INLINE_BYTES: usize>(
        kv_store: &Self::KVStore,
        range: &KeyRange<Bytes<'a, INLINE_BYTES>>,
        storage_counters: StorageCounters,
    ) -> Self;
}

impl<T: KVStoreRangeIterator> Poolable for T {}

pub(crate) enum ContinueCondition {
    ExactPrefix(ByteArray<48>),
    EndPrefixInclusive(ByteArray<48>),
    EndPrefixExclusive(ByteArray<48>),
    Always,
}
