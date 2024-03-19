/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;

pub trait Prefix: Ord + Clone + Debug {
    fn starts_with(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone)]
pub struct PrefixRange<T>
    where
        T: Prefix
{
    // inclusive
    start: T,
    end: Option<RangeEnd<T>>,
}

impl<T: Prefix> PrefixRange<T> {
    pub fn new_unbounded(start: T) -> Self {
        Self { start, end: None }
    }

    pub fn new(start: T, end: RangeEnd<T>) -> Self {
        Self { start, end: Some(end) }
    }

    pub fn new_within(prefix: T) -> Self {
        Self { start: prefix, end: Some(RangeEnd::PrefixSameAsStart) }
    }

    pub fn new_inclusive(start: T, end_inclusive: T) -> Self {
        Self { start, end: Some(RangeEnd::new_inclusive(end_inclusive)) }
    }

    pub fn new_exclusive(start: T, end_exclusive: T) -> Self {
        Self { start, end: Some(RangeEnd::new_exclusive(end_exclusive)) }
    }

    pub fn start(&self) -> &T {
        &self.start
    }

    pub fn end(&self) -> Option<&RangeEnd<T>> {
        self.end.as_ref()
    }

    pub fn into(self) -> (T, Option<RangeEnd<T>>) {
        (self.start, self.end)
    }

    pub fn map<'a, V: Prefix>(self, mapper: impl Fn(T) -> V) -> PrefixRange<V> {
        let (start, end) = self.into();
        let start = mapper(start);
        let end = end.map(|end| end.map(|value| mapper(value)));
        if end.is_some() {
            PrefixRange::new(start, end.unwrap())
        } else {
            PrefixRange::new_unbounded(start)
        }
    }

    pub fn contains(&self, value: T) -> bool {
        if value < *self.start().borrow() {
            false
        } else if let Some(end) = self.end() {
            match end {
                RangeEnd::PrefixSameAsStart => value.starts_with(self.start()),
                RangeEnd::Inclusive(e) => value.cmp(e) == Ordering::Less || value.starts_with(e) ,
                RangeEnd::Exclusive(e) => value.cmp(e) == Ordering::Less
            }
        } else {
            true
        }
    }

    fn end_value(&self) -> Option<&T> {
        match &self.end {
            None => None,
            Some(RangeEnd::PrefixSameAsStart) => Some(self.start()),
            Some(RangeEnd::Inclusive(value)) => Some(value),
            Some(RangeEnd::Exclusive(value)) => Some(value),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RangeEnd<T> where T: Ord + Debug {
    PrefixSameAsStart,
    Inclusive(T),
    Exclusive(T),
}

impl<T> RangeEnd<T> where T: Ord + Debug {
    pub fn new_same_as_start() -> Self {
        RangeEnd::PrefixSameAsStart
    }

    pub fn new_exclusive(value: T) -> Self {
        RangeEnd::Exclusive(value)
    }

    pub fn new_inclusive(value: T) -> Self {
        RangeEnd::Inclusive(value)
    }

    fn is_inclusive(&self) -> bool {
        !matches!(self, Self::Exclusive(_))
    }

    pub fn map<U: Ord + Debug>(self, mapper: impl FnOnce(T) -> U) -> RangeEnd<U> {
        match self {
            RangeEnd::PrefixSameAsStart => RangeEnd::PrefixSameAsStart,
            RangeEnd::Inclusive(end) => RangeEnd::Inclusive(mapper(end)),
            RangeEnd::Exclusive(end) => RangeEnd::Exclusive(mapper(end)),
        }
    }
}