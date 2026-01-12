/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering, collections::BinaryHeap, mem};

use crate::{higher_order::FnHktHelper, LendingIterator, Peekable, Seekable};

pub struct KMergeBy<I: LendingIterator, F> {
    pub iterators: BinaryHeap<PeekWrapper<I, F>>,
    pub next_iterator: Option<PeekWrapper<I, F>>,
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
        Self { iterators: queue, next_iterator: None }
    }

    pub fn find_next_state(&mut self) -> Option<()> {
        if let Some(mut last_iterator) = self.next_iterator.take() {
            if last_iterator.iter.peek().is_some() {
                self.iterators.push(last_iterator);
            }
        }
        self.iterators.pop().map(|iterator| self.next_iterator = Some(iterator))
    }

    pub fn is_done(&self) -> bool {
        self.next_iterator.is_none() && self.iterators.is_empty()
    }
}

impl<I, F> LendingIterator for KMergeBy<I, F>
where
    I: LendingIterator,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + Copy + 'static,
{
    type Item<'a> = I::Item<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.find_next_state()?;
        self.next_iterator.as_mut().unwrap().iter.next()
    }
}

impl<I, F, K> Seekable<K> for KMergeBy<I, F>
where
    I: LendingIterator + Seekable<K>,
    F: for<'a, 'b> FnHktHelper<(&'a I::Item<'a>, &'b I::Item<'b>), Ordering> + Copy + 'static,
{
    fn seek(&mut self, key: &K) {
        if self.is_done() {
            return;
        }

        // force recomputation of heap element
        self.iterators = mem::take(&mut self.iterators)
            .into_iter()
            .chain(self.next_iterator.take())
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

#[cfg(test)]
mod tests {
    use std::iter::Peekable;

    use super::*;

    struct Iter<I: Iterator<Item = u64> + 'static>(Peekable<I>);

    impl<I: Iterator<Item = u64> + 'static> Iter<I> {
        fn new(iter: I) -> Self {
            Self(iter.peekable())
        }
    }

    impl<I> LendingIterator for Iter<I>
    where
        I: Iterator<Item = u64> + 'static,
    {
        type Item<'a> = u64;

        fn next(&mut self) -> Option<Self::Item<'_>> {
            self.0.next()
        }
    }

    impl<I> Seekable<u64> for Iter<I>
    where
        I: Iterator<Item = u64> + 'static,
    {
        fn seek(&mut self, key: &u64) {
            while self.0.peek().is_some_and(|x| x < key) {
                self.0.next();
            }
        }

        fn compare_key(&self, item: &Self::Item<'_>, key: &u64) -> Ordering {
            item.cmp(key)
        }
    }

    #[test]
    fn empty_merge() {
        let iters: [Iter<std::iter::Once<u64>>; 0] = [];
        let mut kmerge = KMergeBy::new(iters, |(a, b)| u64::cmp(a, b));
        assert_eq!(kmerge.next(), None);
    }

    #[test]
    fn merge_one() {
        let iters = [Iter::new(0..2)];
        let mut kmerge = KMergeBy::new(iters, |(a, b)| u64::cmp(a, b));
        assert_eq!(kmerge.next(), Some(0));
        assert_eq!(kmerge.next(), Some(1));
        assert_eq!(kmerge.next(), None);
    }

    #[test]
    fn merge_two() {
        let iters = [|x: &u64| x % 2 == 0, |x: &u64| x % 2 == 1]
            .map(|f| Iter::new(0..4).filter::<Box<_>, dyn for<'a> FnHktHelper<&'a u64, bool>>(Box::new(f) as _));
        let mut kmerge = KMergeBy::new(iters, |(a, b)| u64::cmp(a, b));
        assert_eq!(kmerge.next(), Some(0));
        assert_eq!(kmerge.next(), Some(1));
        assert_eq!(kmerge.next(), Some(2));
        assert_eq!(kmerge.next(), Some(3));
        assert_eq!(kmerge.next(), None);
    }

    #[test]
    fn empty_seek() {
        let iters = [Iter::new(0..0)];
        let mut kmerge = KMergeBy::new(iters, |(a, b)| u64::cmp(a, b));
        kmerge.seek(&0);
        assert_eq!(kmerge.next(), None);
    }

    #[test]
    fn double_seek() {
        let iters = [Iter::new(0..2)];
        let mut kmerge = KMergeBy::new(iters, |(a, b)| u64::cmp(a, b));
        assert_eq!(kmerge.next(), Some(0));
        kmerge.seek(&1);
        kmerge.seek(&1);
        assert_eq!(kmerge.next(), Some(1));
        assert_eq!(kmerge.next(), None);
    }
}
