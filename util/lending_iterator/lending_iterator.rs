/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod combinators;
pub mod higher_order;

use crate::{
    combinators::{Filter, Map, TakeWhile},
    higher_order::{FnMutHktHelper, Hkt},
};

pub trait LendingIterator {
    type Item<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>>;
    fn peek(&mut self) -> Option<&Self::Item<'_>>;
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

    fn filter_map<B, F>(self, mapper: F) -> Filter<Map<Self, F, B>, for<'a> fn(&'a Option<B::HktSelf<'_>>) -> bool>
    where
        Self: Sized,
        B: Hkt + 'static,
        F: for<'a> FnMutHktHelper<Self::Item<'a>, Option<B::HktSelf<'a>>>,
    {
        Filter::new(Map::new(self, mapper), |opt| opt.is_some())
    }
}

pub struct Peekable<'a, LI: LendingIterator + 'a> {
    inner: LI,
    peek_item: Option<LI::Item<'a>>,
}

impl<'a, LI: LendingIterator + 'a> Peekable<'a, LI> {
    pub fn new(inner: LI) -> Self {
        Self { inner, peek_item: None }
    }
}
