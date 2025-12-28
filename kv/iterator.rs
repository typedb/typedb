/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, Bytes};
use lending_iterator::{LendingIterator, Seekable};
use primitive::key_range::{KeyRange, RangeEnd, RangeStart};
use resource::profile::StorageCounters;

pub trait KVStoreRangeIterator: LendingIterator + Seekable<[u8]> {
    type Args;
    type Iterpool;

    fn new<'a, const INLINE_BYTES: usize>(
        args: &Self::Args,
        kv_store_name: &'static str,
        range: &KeyRange<Bytes<'a, INLINE_BYTES>>,
        iter_pool: &Self::Iterpool,
        storage_counters: StorageCounters,
    ) -> Self;
}

pub(crate) enum ContinueCondition {
    ExactPrefix(ByteArray<48>),
    EndPrefixInclusive(ByteArray<48>),
    EndPrefixExclusive(ByteArray<48>),
    Always,
}
