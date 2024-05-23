/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use crate::{
    higher_order::{FnMutHktHelper, Hkt},
    LendingIterator,
};

pub struct Map<I, F, B: Hkt> {
    iter: I,
    mapper: F,
    _pd: PhantomData<B>,
}

impl<I, F, B: Hkt> Map<I, F, B> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { iter, mapper, _pd: PhantomData }
    }
}

impl<B: Hkt, I: LendingIterator, F: 'static> LendingIterator for Map<I, F, B>
where
    F: for<'a> FnMutHktHelper<I::Item<'a>, B::HktSelf<'a>>,
{
    type Item<'a> = B::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().map(&mut self.mapper)
    }

    fn seek(&mut self, _: &[u8]) {
        todo!()
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

impl<I: LendingIterator, F: 'static> LendingIterator for Filter<I, F>
where
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
                            std::mem::transmute::<Self::Item<'_>, Self::Item<'_>>(item)
                        });
                    }
                }
            }
        }
    }

    fn seek(&mut self, _: &[u8]) {
        todo!()
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

impl<I: LendingIterator, F: 'static> LendingIterator for TakeWhile<I, F>
where
    F: for<'a, 'b> FnMutHktHelper<&'b I::Item<'a>, bool>,
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
                    std::mem::transmute::<Self::Item<'_>, Self::Item<'_>>(item)
                })
            }
            _ => {
                self.done = true;
                None
            }
        }
    }

    fn seek(&mut self, key: &[u8]) {
        self.iter.seek(key)
    }
}
