/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, io::Read};

use bytes::{byte_array::ByteArray, Bytes};
use lending_iterator::{LendingIterator, Seekable};
use rocksdb::DB;

use crate::{
    key_range::{KeyRange, RangeEnd},
    keyspace::{raw_iterator, raw_iterator::DBIterator, Keyspace, KeyspaceError},
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
        range: KeyRange<Bytes<'a, INLINE_BYTES>>,
    ) -> Self {
        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);

        let read_opts = keyspace.new_read_options();
        let kv_storage: &'static DB = unsafe { std::mem::transmute(&keyspace.kv_storage) };
        let mut iterator = raw_iterator::DBIterator::new_from(
            kv_storage.raw_iterator_opt(read_opts),
            range.start().get_value().bytes(),
        );
        if range.start().is_exclusive() {
            Self::may_skip_start(&mut iterator, range.start().get_value().bytes());
        }

        let continue_condition = match range.end() {
            RangeEnd::WithinStartAsPrefix => {
                ContinueCondition::ExactPrefix(ByteArray::from(range.start().get_value().as_reference()))
            }
            RangeEnd::EndPrefixInclusive(end) => {
                ContinueCondition::EndPrefixInclusive(ByteArray::from(end.as_reference()))
            }
            RangeEnd::EndPrefixExclusive(end) => {
                ContinueCondition::EndPrefixExclusive(ByteArray::from(end.as_reference()))
            }
            RangeEnd::Unbounded => ContinueCondition::Always,
        };

        // .take_while(Box::new(move |res: &Result<(&[u8], &[u8]), rocksdb::Error>| match res {
        //     Ok((key, _)) => {
        //         let copy = if key.len() > max_required_length {
        //             ByteArray::copy(&key[0..max_required_length])
        //         } else {
        //             ByteArray::copy(key)
        //         };
        //         range.within_end(&copy)
        //     }
        //     Err(_) => true,
        // }) as Box<_>)
        // .map(error_mapper(keyspace_name))
        // .into_seekable(identity as _, raw_iterator::compare_key as _);

        KeyspaceRangeIterator { iterator, continue_condition, keyspace_name: keyspace.name() }
    }

    fn may_skip_start(iterator: &mut DBIterator, excluded_value: &[u8]) {
        if iterator.peek().is_some_and(|result| result.as_ref().is_ok_and(|(key, _)| *key == excluded_value)) {
            iterator.next();
        }
    }

    fn accept_value(condition: &ContinueCondition, value: &<Self as LendingIterator>::Item<'_>) -> bool {
        match value {
            Ok((key, _)) => {
                match condition {
                    ContinueCondition::ExactPrefix(prefix) => key.starts_with(prefix.bytes()),
                    ContinueCondition::EndPrefixInclusive(end_inclusive) => {
                        // if the key is shorter than the end, and the end starts with the key, then it must be OK
                        //  example: A will be included when searching up to and including AA
                        // otherwise, the key is longer and we check the corresponding ranges
                        end_inclusive.bytes().starts_with(key)
                            || &key[0..end_inclusive.length()] <= end_inclusive.bytes()
                    }
                    ContinueCondition::EndPrefixExclusive(end_exclusive) => {
                        // if the key is shorter than the end, and the end starts with the key, then it must be OK
                        //  example: A will be included when searching up to but not including AA
                        // otherwise, the key is longer and we check the corresponding ranges
                        end_exclusive.bytes().starts_with(key)
                            || &key[0..end_exclusive.length()] < end_exclusive.bytes()
                    }
                    ContinueCondition::Always => true,
                }
            }
            Err(err) => true,
        }
    }
}

fn identity(input: &[u8]) -> &[u8] {
    input
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
