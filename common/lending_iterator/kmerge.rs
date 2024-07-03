/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::BinaryHeap, marker::PhantomData};
use std::borrow::Borrow;

use crate::{higher_order::FnHktHelper, LendingIterator, Peekable};

pub struct KMergeBy<I: LendingIterator, F> {
    iterators: BinaryHeap<PeekWrapper<I, F>>,
    next_iterator: Option<PeekWrapper<I, F>>,
    state: State,
    phantom_compare: PhantomData<F>,
}

#[derive(Copy, Clone)]
enum State {
    Init,
    Used,
    Ready,
    Done,
}

impl<I: LendingIterator, F> KMergeBy<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + Copy + 'static,
{
    pub fn new(mut iters: impl IntoIterator<Item = Peekable<I>>, compare_fn: F) -> Self {
        let iters = iters
            .into_iter()
            .map(|mut peekable| {
                let _ = peekable.peek(); // peek requires a mutable ownership, which we can't do in a filter()
                peekable
            })
            .filter(|peekable| peekable.get_peeked().is_some())
            .map(|peekable| PeekWrapper { iter: peekable, cmp_fn: compare_fn });
        let queue = BinaryHeap::from_iter(iters);
        Self { iterators: queue, next_iterator: None, state: State::Init, phantom_compare: PhantomData::default() }
    }

    fn find_next_state(&mut self) {
        match self.iterators.pop() {
            None => self.state = State::Done,
            Some(iterator) => {
                self.next_iterator = Some(iterator);
                self.state = State::Ready;
            }
        }
    }
}

impl<I, F> LendingIterator for KMergeBy<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + Copy + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.state {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::Used => {
                let mut last_iterator = self.next_iterator.take().unwrap();
                if last_iterator.iter.peek().is_some() {
                    self.iterators.push(last_iterator);
                }
                self.find_next_state();
                self.next()
            }
            State::Ready => {
                self.state = State::Used;
                self.next_iterator.as_mut().unwrap().iter.next()
            }
            State::Done => None,
        }
    }
}

// TODO: Seekable

struct PeekWrapper<I: LendingIterator, F: ?Sized> {
    iter: Peekable<I>,
    cmp_fn: F,
}

impl<I, F> PartialOrd for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.cmp_fn.borrow())((self.iter.get_peeked().unwrap(), other.iter.get_peeked().unwrap())))
    }
}

impl<I, F> Ord for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<I, F> PartialEq for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap().is_eq()
    }
}

impl<I, F> Eq for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: Borrow<dyn for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering>> + 'static,
{
}
