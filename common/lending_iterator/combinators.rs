/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, mem::transmute};

use crate::{
    higher_order::{FnMutHktHelper, Hkt},
    LendingIterator, Seekable,
};

pub struct Map<I, F, B> {
    iter: I,
    mapper: F,
    _pd: PhantomData<B>,
}

impl<I, F, B> Map<I, F, B> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { iter, mapper, _pd: PhantomData }
    }

    pub fn into_seekable<G>(self, unmapper: G) -> SeekableMap<I, F, G, B> {
        let Self { iter, mapper, _pd } = self;
        SeekableMap { iter, mapper, unmapper, _pd }
    }
}

impl<I, F, B> LendingIterator for Map<I, F, B>
where
    B: Hkt,
    I: LendingIterator,
    F: for<'a> FnMutHktHelper<I::Item<'a>, B::HktSelf<'a>>,
{
    type Item<'a> = B::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().map(&mut self.mapper)
    }
}

pub struct SeekableMap<I, F, G, B> {
    iter: I,
    mapper: F,
    unmapper: G,
    _pd: PhantomData<B>,
}

impl<I, F, G, B> LendingIterator for SeekableMap<I, F, G, B>
where
    B: Hkt,
    I: LendingIterator,
    F: for<'a> FnMutHktHelper<I::Item<'a>, B::HktSelf<'a>>,
    G: 'static,
{
    type Item<'a> = B::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().map(&mut self.mapper)
    }
}

impl<I, F, G, B, M: ?Sized, K: ?Sized> Seekable<M> for SeekableMap<I, F, G, B>
where
    Self: LendingIterator,
    I: Seekable<K>,
    G: FnMut(&M) -> &K,
{
    fn seek(&mut self, key: &M) {
        self.iter.seek((self.unmapper)(key))
    }
}

pub struct Filter<I, F> {
    iter: I,
    pred: F,
}

impl<I, F> Filter<I, F> {
    pub(crate) fn new(iter: I, pred: F) -> Self {
        Self { iter, pred }
    }
}

impl<I, F> LendingIterator for Filter<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnMutHktHelper<&'b I::Item<'a>, bool>,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.iter.next() {
                None => return None,
                Some(item) => {
                    if (self.pred)(&item) {
                        return Some(unsafe {
                            // SAFETY: this transmutes from Item to Item to extend the lifetime
                            // to the borrow of `self` and immediately return.
                            // The underlying lending iterator cannot be advanced before self.next() is called again,
                            // which will force this borrow to be released.
                            transmute::<Self::Item<'_>, Self::Item<'_>>(item)
                        });
                    }
                }
            }
        }
    }
}

impl<I, F, K> Seekable<K> for Filter<I, F>
where
    I: Seekable<K>,
    F: for<'a, 'b> FnMutHktHelper<&'b I::Item<'a>, bool>,
{
    fn seek(&mut self, key: &K) {
        self.iter.seek(key)
    }
}

pub struct TakeWhile<I, F> {
    iter: I,
    pred: F,
    done: bool,
}

impl<I, F> TakeWhile<I, F> {
    pub(crate) fn new(iter: I, pred: F) -> Self {
        Self { iter, pred, done: false }
    }
}

impl<I, F> LendingIterator for TakeWhile<I, F>
where
    F: for<'a, 'b> FnMutHktHelper<&'b I::Item<'a>, bool>,
    I: LendingIterator,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.done {
            return None;
        }

        match self.iter.next() {
            Some(item) if (self.pred)(&item) => {
                Some(unsafe {
                    // SAFETY: this transmutes from Item to Item to extend the lifetime
                    // to the borrow of `self` and immediately return.
                    // The underlying lending iterator cannot be advanced before self.next() is called again,
                    // which will force this borrow to be released.
                    transmute::<Self::Item<'_>, Self::Item<'_>>(item)
                })
            }
            _ => {
                self.done = true;
                None
            }
        }
    }
}

impl<I, F, K: ?Sized> Seekable<K> for TakeWhile<I, F>
where
    F: for<'a, 'b> FnMutHktHelper<&'b I::Item<'a>, bool>,
    I: Seekable<K>,
{
    /// Seeks the underlying iterator to next matching item, ignoring the predicate for the intermediate items.
    fn seek(&mut self, key: &K) {
        self.iter.seek(key)
    }
}
