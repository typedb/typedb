/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod combinators;
pub mod higher_order;

use std::{cmp::Ordering, iter, mem::transmute};
use std::borrow::Borrow;

use combinators::FilterMap;
use higher_order::AdHocHkt;

use crate::{
    combinators::{Filter, Map, TakeWhile},
    higher_order::{FnMutHktHelper, Hkt},
};
use crate::higher_order::FnHktHelper;

pub trait LendingIterator: 'static {
    type Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>>;

    fn filter<P, F>(self, pred: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: Borrow<F>,
        F: for<'a, 'b> FnHktHelper<&'a Self::Item<'b>, bool> + ?Sized
    {
        Filter::new(self, pred)
    }

    fn take_while<P>(self, pred: P) -> TakeWhile<Self, P>
    where
        Self: Sized,
        P: FnMut(&Self::Item<'_>) -> bool,
    {
        TakeWhile::new(self, pred)
    }

    fn map<B, F>(self, mapper: F) -> Map<Self, F, B>
    where
        Self: Sized,
        B: Hkt + 'static,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, B::HktSelf<'a>>,
    {
        Map::new(self, mapper)
    }

    fn map_static<B, F>(self, mapper: F) -> Map<Self, F, AdHocHkt<B>>
    where
        Self: Sized,
        B: 'static,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, B>,
    {
        Map::new(self, mapper)
    }

    fn filter_map<B, F>(self, mapper: F) -> FilterMap<Self, F, B>
    where
        Self: Sized,
        B: Hkt + 'static,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, Option<B::HktSelf<'a>>>,
    {
        FilterMap::new(self, mapper)
    }

    fn into_iter(mut self) -> impl Iterator<Item = Self::Item<'static>>
    where
        Self: Sized,
        for<'a> Self::Item<'a>: 'static,
    {
        iter::from_fn(move || unsafe {
            // SAFETY: `Self::Item<'a>: 'static` implies that the item is independent from the iterator.
            transmute::<Option<Self::Item<'_>>, Option<Self::Item<'static>>>(self.next())
        })
    }

    fn collect<B>(self) -> B
    where
        Self: Sized,
        for<'a> Self::Item<'a>: 'static,
        B: FromIterator<Self::Item<'static>>,
    {
        self.into_iter().collect()
    }

    fn count(mut self) -> usize
    where
        Self: Sized,
    {
        let mut count = 0;
        while self.next().is_some() {
            count += 1;
        }
        count
    }

    fn try_collect<B, E>(self) -> Result<B, E>
    where
        Self: Sized,
        for<'a> Self::Item<'a>: 'static,
        Result<B, E>: FromIterator<Self::Item<'static>>,
    {
        self.collect()
    }
}

pub trait Seekable<K: ?Sized>: LendingIterator {
    fn seek(&mut self, key: &K);
    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering;
}

pub struct Peekable<LI: LendingIterator> {
    iter: LI,
    item: Option<LI::Item<'static>>,
}

impl<LI: LendingIterator> Peekable<LI> {
    pub fn new(iter: LI) -> Self {
        Self { iter, item: None }
    }

    pub fn peek(&mut self) -> Option<&LI::Item<'_>> {
        if self.item.is_none() {
            self.item = unsafe {
                // SAFETY: the stored item is only accessible while mutably borrowing this iterator.
                // When the underlying iterator is advanced, the stored item is discarded.
                transmute::<Option<LI::Item<'_>>, Option<LI::Item<'static>>>(self.iter.next())
            };
        }
        unsafe {
            // SAFETY: the item reference borrows this iterator mutably. This iterator cannot be advanced while it exists.
            transmute::<Option<&LI::Item<'static>>, Option<&LI::Item<'_>>>(self.item.as_ref())
        }
    }
}

impl<LI: LendingIterator> LendingIterator for Peekable<LI> {
    type Item<'a> = LI::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.item.take() {
            Some(item) => Some(unsafe {
                // SAFETY: the item borrows this iterator mutably. This iterator cannot be advanced while it exists.
                transmute::<LI::Item<'static>, LI::Item<'_>>(item)
            }),
            None => self.iter.next(),
        }
    }
}

impl<K: ?Sized, LI> Seekable<K> for Peekable<LI>
where
    LI: Seekable<K>,
{
    fn seek(&mut self, key: &K) {
        if self.item.is_some() {
            let item = self.item.as_ref().unwrap();
            match self.compare_key(item, key) {
                Ordering::Less => {
                    unreachable!("Key behind the stored item in a Peekable iterator")
                }
                Ordering::Equal => return,
                Ordering::Greater => (), // fallthrough
            }
        }
        self.item = None;
        self.iter.seek(key)
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        self.iter.compare_key(item, key)
    }
}
