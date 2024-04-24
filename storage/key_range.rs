/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering, fmt::Debug};
use primitive::prefix::Prefix;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange<T: Prefix> {
    // inclusive
    start: T,
    end: RangeEnd<T>,
    fixed_width_keys: bool,
}

impl<T: Prefix> KeyRange<T> {
    pub fn new(start: T, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: false }
    }

    pub fn new_fixed_width(start: T, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: true }
    }

    pub fn new_unbounded(start: T) -> Self {
        Self { start, end: RangeEnd::Unbounded, fixed_width_keys: false }
    }

    pub fn new_within(prefix: T, fixed_width_keys: bool) -> Self {
        Self { start: prefix, end: RangeEnd::SameAsStart, fixed_width_keys: false }
    }

    pub fn new_inclusive(start: T, end_inclusive: T) -> Self {
        Self { start, end: RangeEnd::new_inclusive(end_inclusive), fixed_width_keys: false }
    }

    pub fn new_exclusive(start: T, end_exclusive: T) -> Self {
        Self { start, end: RangeEnd::new_exclusive(end_exclusive), fixed_width_keys: false }
    }

    pub fn start(&self) -> &T {
        &self.start
    }

    pub fn end(&self) -> &RangeEnd<T> {
        &self.end
    }

    pub fn fixed_width(&self) -> bool {
        self.fixed_width_keys
    }

    pub fn into_raw(self) -> (T, RangeEnd<T>, bool) {
        (self.start, self.end, self.fixed_width_keys)
    }

    pub fn map<V: Prefix>(self, prefix_mapper: impl Fn(T) -> V, fixed_width_mapper: impl Fn(bool) -> bool) -> KeyRange<V> {
        let (start, end, fixed_width) = self.into_raw();
        let start = prefix_mapper(start);
        let end = end.map(prefix_mapper);
        let fixed_width = fixed_width_mapper(fixed_width);
        match fixed_width {
            true => KeyRange::new_fixed_width(start, end),
            false => KeyRange::new(start, end)
        }
    }

    pub fn contains(&self, value: T) -> bool {
        if value < *self.start().borrow() {
            false
        } else {
            self.within_end(value)
        }
    }

    pub fn within_end(&self, value: T) -> bool {
        match &self.end {
            RangeEnd::SameAsStart => value.starts_with(self.start()),
            RangeEnd::Inclusive(e) => value.cmp(e) == Ordering::Less || value.starts_with(e),
            RangeEnd::Exclusive(e) => value.cmp(e) == Ordering::Less,
            RangeEnd::Unbounded => true,
        }
    }

    fn end_value(&self) -> Option<&T> {
        match &self.end {
            RangeEnd::SameAsStart => Some(self.start()),
            RangeEnd::Inclusive(value) => Some(value),
            RangeEnd::Exclusive(value) => Some(value),
            RangeEnd::Unbounded => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeEnd<T>
    where
        T: Ord + Debug,
{
    SameAsStart,
    Inclusive(T),
    Exclusive(T),
    Unbounded,
}

impl<T> RangeEnd<T>
    where
        T: Ord + Debug,
{
    pub fn new_same_as_start() -> Self {
        RangeEnd::SameAsStart
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
            RangeEnd::SameAsStart => RangeEnd::SameAsStart,
            RangeEnd::Inclusive(end) => RangeEnd::Inclusive(mapper(end)),
            RangeEnd::Exclusive(end) => RangeEnd::Exclusive(mapper(end)),
            RangeEnd::Unbounded => RangeEnd::Unbounded,
        }
    }
}

