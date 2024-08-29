/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod adaptors;
pub mod higher_order;
pub mod kmerge;

use std::{borrow::Borrow, cmp::Ordering, iter, mem::transmute, vec};
use std::vec::IntoIter;

use crate::{
    adaptors::{Chain, Filter, FilterMap, FlatMap, Flatten, Map, TakeWhile, TryFilter, TryFlatMap},
    higher_order::{AdHocHkt, FnHktHelper, FnMutHktHelper, Hkt},
};

pub trait LendingIterator: 'static {
    type Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>>;

    fn chain<Other>(self, other: Other) -> Chain<Self, Other>
    where
        Self: Sized,
        Other: for<'a> LendingIterator<Item<'a> = Self::Item<'a>>,
    {
        Chain::new(self, other)
    }

    fn filter<P, F>(self, pred: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: Borrow<F>,
        F: for<'a, 'b> FnHktHelper<&'a Self::Item<'b>, bool> + ?Sized,
    {
        Filter::new(self, pred)
    }

    fn try_filter<P, F, T, E>(self, pred: P) -> TryFilter<Self, P, T, E>
    where
        T: Hkt,
        Self: Sized + for<'a> LendingIterator<Item<'a> = Result<T::HktSelf<'a>, E>>,
        P: Borrow<F>,
        F: for<'a, 'b> FnHktHelper<&'a Self::Item<'b>, Result<bool, E>> + ?Sized,
    {
        TryFilter::new(self, pred)
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

    fn flatten(self) -> Flatten<Self>
    where
        Self: Sized,
        Self::Item<'static>: LendingIterator,
    {
        Flatten::new(self)
    }

    fn flat_map<J, F>(self, mapper: F) -> FlatMap<Self, J, F>
    where
        Self: Sized,
        J: LendingIterator,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, J>,
    {
        FlatMap::new(self, mapper)
    }

    fn try_flat_map<J, F, T, E>(self, mapper: F) -> TryFlatMap<Self, J, F, T, E>
    where
        Self: Sized,
        J: for<'a> LendingIterator<Item<'a> = Result<T, E>>,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, Result<J, E>>,
    {
        TryFlatMap::new(self, mapper)
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

    fn try_collect<B, E>(self) -> Result<B, E>
    where
        Self: Sized,
        for<'a> Self::Item<'a>: 'static,
        Result<B, E>: FromIterator<Self::Item<'static>>,
    {
        self.collect()
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

    fn count_as_ref(&mut self) -> usize
    where
        Self: Sized,
    {
        let mut count = 0;
        while self.next().is_some() {
            count += 1;
        }
        count
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
        self.get_peeked()
    }

    pub(crate) fn get_peeked(&self) -> Option<&LI::Item<'_>> {
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

pub struct AsLendingIterator<I: Iterator> {
    iter: I,
}

impl<I: Iterator> AsLendingIterator<I> {
    pub fn new(iter: impl IntoIterator<IntoIter = I>) -> Self {
        AsLendingIterator { iter: iter.into_iter() }
    }
}

impl<I: Iterator + 'static> LendingIterator for AsLendingIterator<I> {
    type Item<'a> = I::Item;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next()
    }
}

pub struct Once<T: Hkt> {
    inner: Option<T::HktSelf<'static>>,
}

pub fn once<T: Hkt>(inner: T::HktSelf<'static>) -> Once<T> {
    Once { inner: Some(inner) }
}

impl<T: Hkt> LendingIterator for Once<T> {
    type Item<'a> = T::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.inner.take().map(|item| unsafe {
            // SAFETY: this strictly narrows the lifetime
            transmute::<Self::Item<'static>, Self::Item<'_>>(item)
        })
    }
}
