/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashSet},
    hash::Hash,
};

#[derive(Debug, Clone)]
pub enum State<E> {
    Init,
    ItemReady,
    ItemUsed,
    Error(E),
    Done,
}

pub trait Collector<T> {
    fn add(&mut self, element: T);
}

impl<T> Collector<T> for Vec<T> {
    fn add(&mut self, element: T) {
        self.push(element);
    }
}

impl<T: PartialOrd + Ord> Collector<T> for BTreeSet<T> {
    fn add(&mut self, element: T) {
        self.insert(element);
    }
}

impl<T: Hash + Eq + PartialEq> Collector<T> for HashSet<T> {
    fn add(&mut self, element: T) {
        self.insert(element);
    }
}
