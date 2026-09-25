/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    cmp::Ordering,
    collections::{BTreeMap, btree_map::Range},
    ops::Bound,
};

struct BTreeMapAndRange<'a, K: Ord, V> {
    map: &'a BTreeMap<K, V>,
    range: Range<'a, K, V>,
}

impl<'a, K: Ord, V> BTreeMapAndRange<'a, K, V> {
    pub fn new(map: &'a BTreeMap<K, V>) -> Self {
        let range = map.range(..);
        Self { map, range }
    }

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        self.range.next()
    }

    fn seek(&mut self, key: &K) {
        self.range = self.map.range((Bound::Included(key), Bound::Unbounded))
    }
}

// The implementation may rely on keys being unique, which is guaranteed for BTreeMaps
pub struct BTreeMapIntersectionIterator<'a, K: Ord, V1, V2> {
    first: BTreeMapAndRange<'a, K, V1>,
    second: BTreeMapAndRange<'a, K, V2>,
}

impl<'a, K: Ord, V1, V2> BTreeMapIntersectionIterator<'a, K, V1, V2> {
    pub fn new(first: &'a BTreeMap<K, V1>, second: &'a BTreeMap<K, V2>) -> Self {
        Self { first: BTreeMapAndRange::new(first), second: BTreeMapAndRange::new(second) }
    }
}

impl<'a, K: Ord, V1, V2> Iterator for BTreeMapIntersectionIterator<'a, K, V1, V2> {
    type Item = (&'a K, &'a V1, &'a V2);

    fn next(&mut self) -> Option<Self::Item> {
        let mut l = self.first.next()?;
        let mut r = self.second.next()?;

        loop {
            match l.0.cmp(&r.0) {
                Ordering::Equal => return Some((l.0, l.1, r.1)),
                Ordering::Less => {
                    self.first.seek(r.0);
                    l = self.first.next()?;
                }
                Ordering::Greater => {
                    self.second.seek(l.0);
                    r = self.second.next()?;
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::BTreeMap, fmt::Debug};

    use crate::btreemap_intersection_iterator::BTreeMapIntersectionIterator;

    fn check_intersection_is<K: Ord + Eq + Debug, V1: Eq + Debug, V2: Eq + Debug>(
        first: &BTreeMap<K, V1>,
        second: &BTreeMap<K, V2>,
        expected: Vec<(K, V1, V2)>,
    ) {
        let lr: Vec<_> = BTreeMapIntersectionIterator::new(first, second).collect();
        assert_eq!(lr.len(), expected.len());
        lr.into_iter().zip(expected.iter()).for_each(|(act, exp)| assert_eq!(act, (&exp.0, &exp.1, &exp.2)));

        let rl: Vec<_> = BTreeMapIntersectionIterator::new(second, first).collect();
        assert_eq!(rl.len(), expected.len());
        rl.into_iter().zip(expected.iter()).for_each(|(act, exp)| assert_eq!(act, (&exp.0, &exp.2, &exp.1)));
    }

    #[test]
    fn test_identical_keys() {
        check_intersection_is(
            &BTreeMap::from([(0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16)]),
            &BTreeMap::from([(0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26)]),
            vec![(0, 10, 20), (1, 11, 21), (2, 12, 22), (3, 13, 23), (4, 14, 24), (5, 15, 25), (6, 16, 26)],
        )
    }

    #[test]
    fn test_subset() {
        check_intersection_is(
            &BTreeMap::from([(1, 11), (3, 13), (5, 15)]),
            &BTreeMap::from([(0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26)]),
            vec![(1, 11, 21), (3, 13, 23), (5, 15, 25)],
        )
    }

    #[test]
    fn test_no_overlap() {
        check_intersection_is(
            &BTreeMap::from([(0, 10), (2, 12), (4, 14)]),
            &BTreeMap::from([(1, 21), (3, 23), (5, 25)]),
            vec![],
        )
    }

    #[test]
    fn test_one_empty() {
        check_intersection_is::<usize, usize, usize>(
            &BTreeMap::from([]),
            &BTreeMap::from([(0, 20), (1, 21), (2, 22)]),
            vec![],
        )
    }
}
