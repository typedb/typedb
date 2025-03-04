/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem::transmute};

use crate::{
    higher_order::{FnHktHelper, FnMutHktHelper, Hkt},
    LendingIterator, Peekable, Seekable,
};

pub struct Chain<I1, I2> {
    iter_1: I1,
    iter_2: I2,
}

impl<I1, I2> Chain<I1, I2> {
    pub fn new(iter_1: I1, iter_2: I2) -> Self {
        Self { iter_1, iter_2 }
    }
}

impl<I1, I2> LendingIterator for Chain<I1, I2>
where
    I1: LendingIterator,
    I2: for<'a> LendingIterator<Item<'a> = I1::Item<'a>>,
{
    type Item<'a> = I1::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter_1.next().or_else(|| self.iter_2.next())
    }
}

pub struct RepeatEach<I: LendingIterator> {
    inner: Peekable<I>,
    repeats: usize,
    repeated_last: usize,
}

impl<I: LendingIterator> RepeatEach<I> {
    pub fn new(inner: I, repeats: usize) -> Self {
        Self { inner: Peekable::new(inner), repeats, repeated_last: 0 }
    }
}

impl<I> LendingIterator for RepeatEach<I>
where
    I: LendingIterator,
{
    type Item<'a> = &'a I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.repeated_last < self.repeats {
            self.repeated_last += 1;
        } else {
            self.repeated_last = 1;
            self.inner.next();
        }
        self.inner.peek()
    }
}

pub struct Zip<I1, I2> {
    iter_1: I1,
    iter_2: I2,
}

impl<I1, I2> Zip<I1, I2> {
    pub fn new(iter_1: I1, iter_2: I2) -> Self {
        Self { iter_1, iter_2 }
    }
}

impl<I1, I2> LendingIterator for Zip<I1, I2>
where
    I1: LendingIterator,
    I2: LendingIterator,
{
    type Item<'a> = (I1::Item<'a>, I2::Item<'a>);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        Some((self.iter_1.next()?, self.iter_2.next()?))
    }
}

pub struct Inspect<I, F> {
    pub iter: I,
    f: F,
}

impl<I, F> Inspect<I, F> {
    pub fn new(iter: I, mapper: F) -> Self {
        Self { iter, f: mapper }
    }
}

impl<I, F> LendingIterator for Inspect<I, F>
where
    I: LendingIterator,
    F: FnMut(&I::Item<'_>) + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().inspect(&mut self.f)
    }
}

impl<I, F, K> Seekable<K> for Inspect<I, F>
where
    I: LendingIterator + Seekable<K>,
    F: FnMut(&I::Item<'_>) + 'static,
{
    fn seek(&mut self, key: &K) {
        self.iter.seek(key)
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        self.iter.compare_key(item, key)
    }
}

pub struct Map<I, F, B> {
    iter: I,
    mapper: F,
    _pd: PhantomData<B>,
}

impl<I, F, B> Map<I, F, B> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { iter, mapper, _pd: PhantomData }
    }

    pub fn into_seekable<G, Cmp>(self, unmapper: G, comparator: Cmp) -> SeekableMap<I, F, G, B, Cmp> {
        let Self { iter, mapper, _pd } = self;
        SeekableMap { iter, mapper, unmapper, comparator, _pd }
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

pub struct SeekableMap<I, F, G, B, Cmp> {
    iter: I,
    mapper: F,
    unmapper: G,
    comparator: Cmp,
    _pd: PhantomData<B>,
}

impl<I, F, G, B, Cmp> LendingIterator for SeekableMap<I, F, G, B, Cmp>
where
    B: Hkt,
    I: LendingIterator,
    F: for<'a> FnMutHktHelper<I::Item<'a>, B::HktSelf<'a>>,
    G: 'static,
    Cmp: 'static,
{
    type Item<'a> = B::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().map(&mut self.mapper)
    }
}

impl<I, F, G, Cmp, B, M: ?Sized, K: ?Sized> Seekable<M> for SeekableMap<I, F, G, B, Cmp>
where
    Self: LendingIterator,
    I: Seekable<K>,
    G: FnMut(&M) -> &K,
    Cmp: Fn(&Self::Item<'_>, &M) -> Ordering,
{
    fn seek(&mut self, key: &M) {
        self.iter.seek((self.unmapper)(key))
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &M) -> Ordering {
        (self.comparator)(item, key)
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

impl<I, P> LendingIterator for Filter<I, P>
where
    I: LendingIterator,
    P: Borrow<dyn for<'a, 'b> FnHktHelper<&'a I::Item<'b>, bool>> + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.iter.next() {
                None => return None,
                Some(item) => {
                    if (self.pred.borrow())(&item) {
                        return Some(unsafe {
                            // SAFETY: this transmutes from Item to Item to extend the lifetime
                            // to the borrow of `self` and immediately return.
                            // The underlying lending iterator cannot be advanced before self.next() is called again,
                            // which will force this borrow to be released.
                            transmute::<I::Item<'_>, I::Item<'_>>(item)
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
    F: Borrow<dyn for<'a, 'b> FnHktHelper<&'b I::Item<'a>, bool>> + 'static,
{
    fn seek(&mut self, key: &K) {
        self.iter.seek(key)
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        self.iter.compare_key(item, key)
    }
}

pub struct TryFilter<I, F, T, E> {
    iter: I,
    pred: F,
    done: bool,
    _res: PhantomData<Result<T, E>>,
}

impl<I, F, T, E> TryFilter<I, F, T, E> {
    pub(crate) fn new(iter: I, pred: F) -> Self {
        Self { iter, pred, done: false, _res: PhantomData }
    }
}

impl<I, P, T, E> LendingIterator for TryFilter<I, P, T, E>
where
    T: Hkt,
    E: 'static,
    I: for<'a> LendingIterator<Item<'a> = Result<T::HktSelf<'a>, E>>,
    P: Borrow<dyn for<'a, 'b> FnHktHelper<&'a I::Item<'b>, Result<bool, E>>> + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.done {
            return None;
        }
        loop {
            match self.iter.next() {
                None => return None,
                Some(item) => {
                    match (self.pred.borrow())(&item) {
                        Ok(true) => {
                            return Some(unsafe {
                                // SAFETY: this transmutes from Item to Item to extend the lifetime
                                // to the borrow of `self` and immediately return.
                                // The underlying lending iterator cannot be advanced before self.next() is called again,
                                // which will force this borrow to be released.
                                transmute::<I::Item<'_>, I::Item<'_>>(item)
                            });
                        }
                        Ok(false) => (),
                        Err(err) => {
                            self.done = true;
                            return Some(Err(err));
                        }
                    }
                }
            }
        }
    }
}

impl<I, F, T, E, K> Seekable<K> for TryFilter<I, F, T, E>
where
    I: Seekable<K>,
    TryFilter<I, F, T, E>: for<'a> LendingIterator<Item<'a> = I::Item<'a>>,
{
    fn seek(&mut self, key: &K) {
        self.iter.seek(key)
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        self.iter.compare_key(item, key)
    }
}

pub struct FilterMap<I, F, B> {
    iter: I,
    mapper: F,
    _pd: PhantomData<B>,
}

impl<I, F, B> FilterMap<I, F, B> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { iter, mapper, _pd: PhantomData }
    }
}

impl<I, F, B> LendingIterator for FilterMap<I, F, B>
where
    B: Hkt,
    I: LendingIterator,
    F: for<'a> FnMutHktHelper<I::Item<'a>, Option<B::HktSelf<'a>>>,
{
    type Item<'a> = B::HktSelf<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.iter.next() {
                None => return None,
                Some(item) => {
                    if let Some(mapped) = (self.mapper)(item) {
                        return Some(unsafe {
                            // SAFETY: this transmutes from Item to Item to extend the lifetime
                            // to the borrow of `self` and immediately return.
                            // The underlying lending iterator cannot be advanced before self.next() is called again,
                            // which will force this borrow to be released.
                            transmute::<Self::Item<'_>, Self::Item<'_>>(mapped)
                        });
                    }
                }
            }
        }
    }
}

pub struct Flatten<I: LendingIterator<Item<'static>: LendingIterator>> {
    source_iter: I,
    next_iter: Option<Peekable<I::Item<'static>>>,
}

impl<I: LendingIterator<Item<'static>: LendingIterator>> Flatten<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { source_iter: iter, next_iter: None }
    }
}

impl<I: for<'a> LendingIterator<Item<'a>: LendingIterator>> LendingIterator for Flatten<I> {
    type Item<'a> = <I::Item<'static> as LendingIterator>::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.next_iter.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            let source_item = self.source_iter.next()?;
            // SAFETY: We only advance source_iter once next_iter is exhausted
            let source_item = unsafe { std::mem::transmute::<I::Item<'_>, I::Item<'static>>(source_item) };
            self.next_iter = Some(Peekable::new(source_item));
        }
        self.next_iter.as_mut().unwrap().next()
    }
}

pub struct FlatMap<I, J: LendingIterator, F> {
    source_iter: I,
    next_iter: Option<Peekable<J>>,
    mapper: F,
}

impl<I, J: LendingIterator, F> FlatMap<I, J, F> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { source_iter: iter, mapper, next_iter: None }
    }
}

impl<I, J, F> LendingIterator for FlatMap<I, J, F>
where
    I: LendingIterator,
    J: LendingIterator,
    F: for<'a> FnMutHktHelper<I::Item<'a>, J>,
{
    type Item<'a> = J::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.next_iter.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            match self.source_iter.next() {
                None => return None,
                Some(source_item) => {
                    self.next_iter = Some(Peekable::new((self.mapper)(source_item)));
                }
            }
        }
        self.next_iter.as_mut().unwrap().next()
    }
}

pub struct TryFlatMap<I, J: LendingIterator, F, T, E> {
    source_iter: I,
    next_iter: Option<Peekable<J>>,
    mapper: F,
    _pd: PhantomData<Result<T, E>>,
}

impl<I, J: LendingIterator, F, T, E> TryFlatMap<I, J, F, T, E> {
    pub(crate) fn new(iter: I, mapper: F) -> Self {
        Self { source_iter: iter, mapper, next_iter: None, _pd: PhantomData }
    }
}

impl<I, J, F, T, E> LendingIterator for TryFlatMap<I, J, F, T, E>
where
    I: LendingIterator,
    J: for<'a> LendingIterator<Item<'a> = Result<T::HktSelf<'a>, E>>,
    F: for<'a> FnMutHktHelper<I::Item<'a>, Result<J, E>>,
    T: Hkt,
    E: 'static,
{
    type Item<'a> = Result<T::HktSelf<'a>, E>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.next_iter.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            match self.source_iter.next() {
                None => return None,
                Some(source_item) => match (self.mapper)(source_item) {
                    Ok(next) => self.next_iter = Some(Peekable::new(next)),
                    Err(err) => return Some(Err(err)),
                },
            }
        }
        self.next_iter.as_mut().unwrap().next()
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

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        self.iter.compare_key(item, key)
    }
}
