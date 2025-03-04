/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, mem::transmute};

use lending_iterator::{LendingIterator, Seekable};
use rocksdb::DBRawIterator;

use crate::snapshot::pool::PoolRecycleGuard;

type KeyValue<'a> = Result<(&'a [u8], &'a [u8]), rocksdb::Error>;

/// SAFETY NOTE: `'static` here represents that the `DBIterator` owns the data.
/// The item's lifetime is in fact invalidated when `iterator` is advanced.
pub(super) struct DBIterator {
    iterator: PoolRecycleGuard<DBRawIterator<'static>>,
    // NOTE: when item is empty, that means that the kv pair the Rocks iterator _is currently pointing to_
    //       has been yielded to the user, and the underlying iterator needs to be advanced before peeking
    item: Option<KeyValue<'static>>,
}

impl DBIterator {
    pub(super) fn new_from(mut iterator: PoolRecycleGuard<DBRawIterator<'static>>, start: &[u8]) -> Self {
        iterator.seek(start);
        let item = peek_item(&iterator);
        Self { iterator, item }
    }

    pub(super) fn peek(&mut self) -> Option<&<Self as LendingIterator>::Item<'_>> {
        match self.next() {
            Some(item) => {
                self.item = Some(unsafe {
                    // SAFETY: the stored item is only accessible while mutably borrowing this iterator.
                    // When the underlying iterator is advanced, the stored item is discarded.
                    transmute::<KeyValue<'_>, KeyValue<'static>>(item)
                });
                self.item.as_ref()
            }
            None => None,
        }
    }
}

fn peek_item(iterator: &DBRawIterator<'static>) -> Option<KeyValue<'static>> {
    if iterator.valid() {
        iterator.item().map(|item| unsafe { transmute::<KeyValue<'_>, KeyValue<'static>>(Ok(item)) })
    } else if let Err(err) = iterator.status() {
        Some(Err(err))
    } else {
        None
    }
}

impl LendingIterator for DBIterator {
    type Item<'a>
        = Result<(&'a [u8], &'a [u8]), rocksdb::Error>
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
        if let Some(Ok((item_key, _))) = self.item.as_ref() {
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
        self.item.take();
        self.iterator.seek(key);
        self.item = peek_item(&self.iterator); // repopulate `item` to prevent advancing the underlying iterator
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
