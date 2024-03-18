/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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

impl<E> PartialEq for State<E> {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl<E> Eq for State<E> {}

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
