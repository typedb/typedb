/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::{byte_array::ByteArray, Bytes};
use lending_iterator::{
    combinators::{SeekableMap, TakeWhile},
    LendingIterator, Peekable, Seekable,
};
use logger::result::ResultExt;
use speedb::DB;

use super::keyspace::{Keyspace, KeyspaceError};
use crate::key_range::{KeyRange, RangeEnd};

mod raw {
    use std::cmp::Ordering;

    use lending_iterator::{LendingIterator, Seekable};
    use speedb::DBRawIterator;

    pub(super) struct DBIterator {
        iterator: DBRawIterator<'static>,
        item: Option<Result<(&'static [u8], &'static [u8]), speedb::Error>>,
    }

    impl DBIterator {
        pub(super) fn new_from(mut iterator: DBRawIterator<'static>, start: &[u8]) -> Self {
            iterator.seek(start);
            let item = iterator.valid().then(|| unsafe { std::mem::transmute(iterator.item()) });
            Self { item: Ok(item).transpose(), iterator }
        }

        fn peek(&mut self) -> Option<&<Self as LendingIterator>::Item<'_>> {
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
    }

    impl Seekable<[u8]> for DBIterator {
        fn seek(&mut self, key: &[u8]) {
            if let Some(item) = self.peek() {
                match compare_key(item, key) {
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
}

pub struct KeyspaceRangeIterator {
    iterator: Peekable<
        SeekableMap<
            TakeWhile<raw::DBIterator, Box<dyn FnMut(&Result<(&[u8], &[u8]), speedb::Error>) -> bool>>,
            Box<
                dyn for<'a> Fn(
                    Result<(&'a [u8], &'a [u8]), speedb::Error>,
                ) -> Result<(&'a [u8], &'a [u8]), KeyspaceError>,
            >,
            fn(&[u8]) -> &[u8],
            Result<(&'static [u8], &'static [u8]), KeyspaceError>,
            fn(&Result<(&[u8], &[u8]), KeyspaceError>, &[u8]) -> Ordering,
        >,
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
                Ok((key, _)) => range.within_end(&ByteArray::copy(key)),
                Err(_) => true,
            }) as Box<_>)
            .map(error_mapper(keyspace_name))
            .into_seekable(
                identity as fn(&[u8]) -> &[u8],
                raw::compare_key::<KeyspaceError> as fn(&Result<(&[u8], &[u8]), KeyspaceError>, &[u8]) -> Ordering,
            );

        KeyspaceRangeIterator { iterator: Peekable::new(range_iterator) }
    }

    pub(crate) fn peek(&mut self) -> Option<&<Self as LendingIterator>::Item<'_>> {
        self.iterator.peek()
    }
}

fn identity(input: &[u8]) -> &[u8] {
    input
}

fn error_mapper(
    keyspace_name: &'static str,
) -> Box<dyn for<'a> Fn(Result<(&'a [u8], &'a [u8]), speedb::Error>) -> Result<(&'a [u8], &'a [u8]), KeyspaceError>> {
    Box::new(move |res| res.map_err(|error| KeyspaceError::Iterate { name: keyspace_name, source: error }))
}

impl LendingIterator for KeyspaceRangeIterator {
    type Item<'a> = Result<(&'a[u8], &'a[u8]), KeyspaceError>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

impl Seekable<[u8]> for KeyspaceRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        self.iterator.compare_key(item, key)
    }
}

impl KeyspaceRangeIterator {
    #[deprecated(note = "use `.map_static(...).collect()` instead")]
    pub fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(
        self,
    ) -> Vec<(ByteArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)> {
        self.iterator
            .map_static::<(ByteArray<INLINE_KEY>, ByteArray<INLINE_VALUE>), _>(
                |res: Result<(&[u8], &[u8]), KeyspaceError>| {
                    let (key, value) = res.unwrap_or_log();
                    (ByteArray::<INLINE_KEY>::copy(key), ByteArray::<INLINE_VALUE>::copy(value))
                },
            )
            .collect()
    }
}
