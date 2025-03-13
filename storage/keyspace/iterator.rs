/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::{byte_array::ByteArray, Bytes};
use lending_iterator::{LendingIterator, Seekable};
use resource::profile::StorageCounters;

use crate::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    keyspace::{raw_iterator, raw_iterator::DBIterator, IteratorPool, Keyspace, KeyspaceError},
};

pub struct KeyspaceRangeIterator {
    iterator: DBIterator,
    continue_condition: ContinueCondition,
    keyspace_name: &'static str,
}

enum ContinueCondition {
    ExactPrefix(ByteArray<48>),
    EndPrefixInclusive(ByteArray<48>),
    EndPrefixExclusive(ByteArray<48>),
    Always,
}

impl KeyspaceRangeIterator {
    pub(crate) fn new<'a, const INLINE_BYTES: usize>(
        keyspace: &'a Keyspace,
        iterpool: &IteratorPool,
        range: &KeyRange<Bytes<'a, INLINE_BYTES>>,
        storage_counters: StorageCounters,
    ) -> Self {
        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);

        let start_prefix = match range.start() {
            RangeStart::Inclusive(bytes) => Bytes::Reference(bytes.as_ref()),
            RangeStart::ExcludeFirstWithPrefix(bytes) => Bytes::Reference(bytes.as_ref()),
            RangeStart::ExcludePrefix(bytes) => {
                let mut cloned = bytes.to_array();
                cloned.increment().unwrap();
                Bytes::Array(cloned)
            }
        };

        let mut iterator = raw_iterator::DBIterator::new_from(
            iterpool.get_iterator(keyspace),
            start_prefix.as_ref(),
            storage_counters,
        );
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
        KeyspaceRangeIterator { iterator, continue_condition, keyspace_name: keyspace.name() }
    }

    fn may_skip_start(iterator: &mut DBIterator, excluded_value: &[u8]) {
        if iterator.peek().is_some_and(|result| result.is_ok_and(|(key, _)| key == excluded_value)) {
            iterator.next();
        }
    }

    fn accept_value(condition: &ContinueCondition, value: &<Self as LendingIterator>::Item<'_>) -> bool {
        match value {
            Ok((key, _)) => {
                match condition {
                    ContinueCondition::ExactPrefix(prefix) => key.starts_with(prefix),
                    ContinueCondition::EndPrefixInclusive(end_inclusive) => {
                        // if the key is shorter than the end, and the end starts with the key, then it must be OK
                        //  example: A will be included when searching up to and including AA
                        // otherwise, the key is longer and we check the corresponding ranges
                        end_inclusive.starts_with(key) || &key[0..end_inclusive.len()] <= end_inclusive
                    }
                    ContinueCondition::EndPrefixExclusive(end_exclusive) => {
                        // if the key is shorter than the end, and the end starts with the key, then it must be OK
                        //  example: A will be included when searching up to but not including AA
                        // otherwise, the key is longer and we check the corresponding ranges
                        end_exclusive.starts_with(key) || &key[0..end_exclusive.len()] < end_exclusive
                    }
                    ContinueCondition::Always => true,
                }
            }
            Err(_err) => true,
        }
    }
}

impl LendingIterator for KeyspaceRangeIterator {
    type Item<'a>
        = Result<(&'a [u8], &'a [u8]), KeyspaceError>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let next = self
            .iterator
            .next()
            .map(|result| result.map_err(|err| KeyspaceError::Iterate { name: self.keyspace_name, source: err }));

        // validate next against the Condition
        match next {
            None => None,
            Some(result) => match Self::accept_value(&self.continue_condition, &result) {
                true => Some(result),
                false => None,
            },
        }
    }
}

impl Seekable<[u8]> for KeyspaceRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        raw_iterator::compare_key(item, key)
    }
}
