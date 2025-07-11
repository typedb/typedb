/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering, collections::BinaryHeap, marker::PhantomData, mem};

use crate::{higher_order::FnHktHelper, LendingIterator, Peekable, Seekable};

pub struct KMergeBy<I: LendingIterator, F> {
    pub iterators: BinaryHeap<PeekWrapper<I, F>>,
    pub next_iterator: Option<PeekWrapper<I, F>>,
    pub state: State,
    phantom_compare: PhantomData<F>,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum State {
    Init,
    Used,
    Ready,
    Done,
}

impl<I: LendingIterator, F> KMergeBy<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + Copy + 'static,
{
    pub fn new(iters: impl IntoIterator<Item = I>, cmp: F) -> Self {
        let iters = iters
            .into_iter()
            .map(|iter| {
                let mut peekable = Peekable::new(iter);
                let _ = peekable.peek(); // peek requires a mutable ownership, which we can't do in a filter()
                peekable
            })
            .filter(|peekable| peekable.get_peeked().is_some())
            .map(|peekable| PeekWrapper { iter: peekable, cmp_fn: cmp });
        // Peek wrapper reverses the comparator to create a min heap
        let queue = BinaryHeap::from_iter(iters);
        Self { iterators: queue, next_iterator: None, state: State::Init, phantom_compare: PhantomData }
    }

    pub fn find_next_state(&mut self) {
        match self.iterators.pop() {
            None => self.state = State::Done,
            Some(iterator) => {
                self.next_iterator = Some(iterator);
                self.state = State::Ready;
            }
        }
    }

    pub fn return_last_to_heap(&mut self) {
        let mut last_iterator = self.next_iterator.take().unwrap();
        if last_iterator.iter.peek().is_some() {
            self.iterators.push(last_iterator);
        }
    }
}

impl<I, F> LendingIterator for KMergeBy<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + Copy + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.state {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::Used => {
                self.return_last_to_heap();
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

impl<I, F, K> Seekable<K> for KMergeBy<I, F>
where
    I: LendingIterator + Seekable<K>,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + Copy + 'static,
{
    fn seek(&mut self, key: &K) {
        if self.state == State::Used {
            self.return_last_to_heap();
            self.find_next_state();
        }
        if let Some(next_iterator) = &mut self.next_iterator {
            next_iterator.iter.seek(key);
        }
        self.iterators = mem::take(&mut self.iterators)
            .drain()
            .filter_map(|mut it| {
                it.iter.peek();
                if let Some(item) = it.iter.get_peeked() {
                    if it.iter.compare_key(item, key) == Ordering::Less {
                        it.iter.seek(key);
                    }
                }
                it.iter.peek().is_some().then_some(it)
            })
            .collect();
        // force recomputation of heap element
        self.state = State::Used;
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &K) -> Ordering {
        if let Some(inner) = self.next_iterator.as_ref().or(self.iterators.peek()) {
            inner.iter.compare_key(item, key)
        } else {
            unreachable!("Called `Seekable::compare_key` on an empty KMergeBy") // no inner iterators
        }
    }
}

pub struct PeekWrapper<I: LendingIterator, F> {
    pub iter: Peekable<I>,
    cmp_fn: F,
}

impl<I, F> PartialOrd for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I, F> Ord for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse the comparator to create a min-ordered heap element
        (self.cmp_fn.borrow())((self.iter.get_peeked().unwrap(), other.iter.get_peeked().unwrap())).reverse()
    }
}

impl<I, F> PartialEq for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap().is_eq()
    }
}

impl<I, F> Eq for PeekWrapper<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + 'static,
{
}
