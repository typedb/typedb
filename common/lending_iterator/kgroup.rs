/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::LendingIterator;

// Merges iterators that are each sorted by the key, lending each run of equal keys across all of them as one group.
// Items are buffered, so they must be owned. The first error ends the iteration.
pub struct KGroupBy<I, T, K, F> {
    iterators: Vec<I>,
    heads: BinaryHeap<Head<K, T>>,
    key_fn: F,
    is_initialised: bool,
    group_key: Option<K>,
    group: Vec<T>,
}

impl<I, T, E, K, F> KGroupBy<I, T, K, F>
where
    I: Iterator<Item = Result<T, E>>,
    K: Ord,
    F: Fn(&T) -> K,
{
    pub fn new(iterators: impl IntoIterator<Item = I>, key_fn: F) -> Self {
        let iterators: Vec<I> = iterators.into_iter().collect();
        Self {
            heads: BinaryHeap::with_capacity(iterators.len()),
            iterators,
            key_fn,
            is_initialised: false,
            group_key: None,
            group: Vec::new(),
        }
    }

    fn find_next_group(&mut self) -> Option<Result<(), E>> {
        if !self.is_initialised {
            self.is_initialised = true;
            for source in 0..self.iterators.len() {
                if let Err(err) = self.advance(source) {
                    return Some(Err(err));
                }
            }
        }

        self.group.clear();
        let Head { key, source, item } = self.heads.pop()?;
        self.group.push(item);
        let mut result = self.advance(source);
        while result.is_ok() && self.heads.peek().is_some_and(|head| head.key == key) {
            let Head { source, item, .. } = self.heads.pop().unwrap();
            self.group.push(item);
            result = self.advance(source);
        }
        self.group_key = Some(key);
        Some(result)
    }

    fn advance(&mut self, source: usize) -> Result<(), E> {
        if let Some(item) = self.iterators[source].next().transpose()? {
            self.heads.push(Head { key: (self.key_fn)(&item), source, item });
        }
        Ok(())
    }
}

impl<I, T, E, K, F> LendingIterator for KGroupBy<I, T, K, F>
where
    I: Iterator<Item = Result<T, E>> + 'static,
    T: 'static,
    E: 'static,
    K: Ord + 'static,
    F: Fn(&T) -> K + 'static,
{
    type Item<'a> = Result<(&'a K, &'a [T]), E>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.find_next_group()? {
            Ok(()) => Some(Ok((self.group_key.as_ref().unwrap(), &self.group))),
            Err(err) => {
                self.heads.clear();
                Some(Err(err))
            }
        }
    }
}

struct Head<K, T> {
    key: K,
    source: usize,
    item: T,
}

// Reversed to make a min heap; ties on the key are broken by source, so a group lists its items in source order
impl<K: Ord, T> Ord for Head<K, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.key, self.source).cmp(&(&other.key, other.source)).reverse()
    }
}

impl<K: Ord, T> PartialOrd for Head<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, T> PartialEq for Head<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<K: Ord, T> Eq for Head<K, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    type Source = std::vec::IntoIter<Result<u64, &'static str>>;

    fn source(items: &[u64]) -> Source {
        items.iter().map(|&item| Ok(item)).collect::<Vec<_>>().into_iter()
    }

    fn collect_groups<F: Fn(&u64) -> u64 + 'static>(
        mut kgroup: KGroupBy<Source, u64, u64, F>,
    ) -> Vec<Result<(u64, Vec<u64>), &'static str>> {
        let mut groups = Vec::new();
        while let Some(group) = kgroup.next() {
            groups.push(group.map(|(key, items)| (*key, items.to_vec())));
        }
        groups
    }

    #[test]
    fn no_iterators() {
        let kgroup = KGroupBy::new(Vec::<Source>::new(), |item: &u64| *item);
        assert_eq!(collect_groups(kgroup), vec![]);
    }

    #[test]
    fn empty_iterators() {
        let kgroup = KGroupBy::new([source(&[]), source(&[])], |item: &u64| *item);
        assert_eq!(collect_groups(kgroup), vec![]);
    }

    #[test]
    fn group_one() {
        let kgroup = KGroupBy::new([source(&[1, 1, 2, 3, 3, 3])], |item: &u64| *item);
        assert_eq!(collect_groups(kgroup), vec![Ok((1, vec![1, 1])), Ok((2, vec![2])), Ok((3, vec![3, 3, 3]))]);
    }

    #[test]
    fn group_across_iterators_in_source_order() {
        let kgroup =
            KGroupBy::new([source(&[10, 21, 30]), source(&[11, 12, 31]), source(&[20])], |item: &u64| *item / 10);
        assert_eq!(
            collect_groups(kgroup),
            vec![Ok((1, vec![10, 11, 12])), Ok((2, vec![21, 20])), Ok((3, vec![30, 31]))]
        );
    }

    #[test]
    fn error_ends_iteration() {
        let failing = vec![Ok(2), Err("broken"), Ok(4)].into_iter();
        let kgroup = KGroupBy::new([source(&[1, 2, 3]), failing], |item: &u64| *item);
        assert_eq!(collect_groups(kgroup), vec![Ok((1, vec![1])), Err("broken")]);
    }

    #[test]
    fn error_on_first_read() {
        let failing = vec![Err("broken")].into_iter();
        let kgroup = KGroupBy::new([source(&[1]), failing], |item: &u64| *item);
        assert_eq!(collect_groups(kgroup), vec![Err("broken")]);
    }
}
