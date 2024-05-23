/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod combinators;
pub mod higher_order;

use std::mem::transmute;

use higher_order::AdHocHkt;

use crate::{
    combinators::{Filter, Map, TakeWhile},
    higher_order::{FnMutHktHelper, Hkt},
};

pub trait LendingIterator: 'static {
    type Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>>;
    fn seek(&mut self, key: &[u8]);

    fn filter<P>(self, pred: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: FnMut(&Self::Item<'_>) -> bool,
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

    fn filter_map<B, F>(self, mapper: F) -> Filter<Map<Self, F, B>, for<'a> fn(&'a Option<B::HktSelf<'_>>) -> bool>
    where
        Self: Sized,
        B: Hkt + 'static,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, Option<B::HktSelf<'a>>>,
    {
        Filter::new(Map::new(self, mapper), |opt| opt.is_some())
    }

    fn collect<B>(mut self) -> B
    where
        Self: Sized,
        for<'a> Self::Item<'a>: 'static,
        B: FromIterator<Self::Item<'static>>,
    {
        std::iter::from_fn(move || unsafe { transmute(self.next()) }).collect()
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
            self.item = unsafe { std::mem::transmute(self.iter.next()) };
        }
        unsafe { std::mem::transmute(self.item.as_ref()) }
    }
}

impl<LI: LendingIterator> LendingIterator for Peekable<LI> {
    type Item<'a> = LI::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if let Some(item) = self.item.take() {
            Some(unsafe { std::mem::transmute(item) })
        } else {
            self.iter.next()
        }
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }
}
