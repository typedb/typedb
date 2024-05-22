/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use lending_iterator::{
    combinators::{Map, TakeWhile},
    LendingIterator,
};
use logger::result::ResultExt;
use speedb::DB;

use super::keyspace::{Keyspace, KeyspaceError};
use crate::key_range::{KeyRange, RangeEnd};

mod raw {
    use std::cmp::Ordering;

    use lending_iterator::LendingIterator;
    use speedb::DBRawIterator;

    pub(super) struct DBIterator {
        iterator: DBRawIterator<'static>,
        item: Option<Result<(&'static [u8], &'static [u8]), speedb::Error>>,
    }

    impl DBIterator {
        pub(super) fn new_from(mut iterator: DBRawIterator<'static>, start: &[u8]) -> Self {
            iterator.seek(start);
            Self { item: Ok(unsafe { std::mem::transmute(iterator.item()) }).transpose(), iterator }
        }
    }

    impl LendingIterator for DBIterator {
        type Item<'a> = Result<(&'a [u8], &'a [u8]), speedb::Error>
        where
            Self: 'a;

        fn next(&mut self) -> Option<Self::Item<'_>> {
            if self.item.is_some() {
                self.item.take()
            } else if self.iterator.valid() {
                self.iterator.next();
                self.iterator.item().map(Ok)
            } else if self.iterator.status().is_err() {
                Some(Err(self.iterator.status().err().unwrap().clone()))
            } else {
                None
            }
        }

        fn peek(&mut self) -> Option<&Self::Item<'_>> {
            match self.next() {
                Some(Ok(item)) => {
                    self.item = Some(unsafe { std::mem::transmute(item) });
                    self.item.as_ref()
                }
                Some(Err(error)) => {
                    self.item = Some(Err(error));
                    self.item.as_ref()
                }
                None => None,
            }
        }

        fn seek(&mut self, key: &[u8]) {
            if let Some(Ok(item)) = self.peek() {
                let (peek, _) = item;
                match peek.cmp(&key) {
                    Ordering::Less => {
                        self.item.take();
                        self.iterator.seek(key);
                    }
                    Ordering::Equal => (),
                    Ordering::Greater => {
                        // TODO: seeking backward could be a no-op or an error or illegal state??
                    }
                }
            }
        }
    }
}

pub struct KeyspaceRangeIterator {
    iterator: Map<
        TakeWhile<raw::DBIterator, Box<dyn FnMut(&Result<(&[u8], &[u8]), speedb::Error>) -> bool>>,
        Box<dyn for<'a> Fn(Result<(&'a [u8], &'a [u8]), speedb::Error>) -> Result<(&'a [u8], &'a [u8]), KeyspaceError>>,
        Result<(&'static [u8], &'static [u8]), KeyspaceError>,
    >,
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
        let iterator = raw::DBIterator::new_from(kv_storage.raw_iterator_opt(read_opts), range.start().bytes());
        let start = range.start().to_array();
        let range = match range.end() {
            RangeEnd::SameAsStart => KeyRange::new_within(start, range.fixed_width()),
            RangeEnd::Inclusive(end) => KeyRange::new_inclusive(start, end.to_array()),
            RangeEnd::Exclusive(end) => KeyRange::new_exclusive(start, end.to_array()),
            RangeEnd::Unbounded => KeyRange::new_unbounded(start),
        };

        let keyspace_name = keyspace.name();

        let range_iterator = iterator
            .take_while(Box::new(move |res: &Result<(&[u8], &[u8]), speedb::Error>| match res {
                Ok((key, _)) => range.within_end(&Bytes::<0>::Reference(ByteReference::new(key))),
                Err(_) => true,
            }) as Box<_>)
            .map(mapper(keyspace_name));

        KeyspaceRangeIterator { iterator: range_iterator }
    }
}

fn mapper(
    name: &'static str,
) -> Box<dyn for<'a> Fn(Result<(&'a [u8], &'a [u8]), speedb::Error>) -> Result<(&'a [u8], &'a [u8]), KeyspaceError>> {
    Box::new(move |res| res.map_err(|error| KeyspaceError::Iterate { name, source: error }))
}

impl LendingIterator for KeyspaceRangeIterator {
    type Item<'a> = Result<(&'a[u8], &'a[u8]), KeyspaceError>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }

    fn peek(&mut self) -> Option<&Self::Item<'_>> {
        self.iterator.peek()
    }

    fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }
}

impl KeyspaceRangeIterator {
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
