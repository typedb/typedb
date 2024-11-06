/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, collections::Bound, fmt::Debug};

use primitive::prefix::Prefix;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange<T: Prefix> {
    // inclusive
    start: RangeStart<T>,
    end: RangeEnd<T>,
    fixed_width_keys: bool,
}

impl<T: Prefix> KeyRange<T> {
    pub fn new_variable_width(start: RangeStart<T>, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: false }
    }

    pub fn new_fixed_width(start: RangeStart<T>, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: true }
    }

    pub fn new_unbounded(start: RangeStart<T>) -> Self {
        Self { start, end: RangeEnd::Unbounded, fixed_width_keys: false }
    }

    pub fn new_within(prefix_inclusive: RangeStart<T>, fixed_width_keys: bool) -> Self {
        Self { start: prefix_inclusive, end: RangeEnd::WithinStartAsPrefix, fixed_width_keys }
    }

    pub fn start(&self) -> &RangeStart<T> {
        &self.start
    }

    pub fn end(&self) -> &RangeEnd<T> {
        &self.end
    }

    pub fn fixed_width(&self) -> bool {
        self.fixed_width_keys
    }

    pub fn into_raw(self) -> (RangeStart<T>, RangeEnd<T>, bool) {
        (self.start, self.end, self.fixed_width_keys)
    }

    pub fn map<V: Prefix>(
        self,
        prefix_mapper: impl Fn(T) -> V,
        fixed_width_mapper: impl Fn(bool) -> bool,
    ) -> KeyRange<V> {
        let (start, end, fixed_width) = self.into_raw();
        let start = start.map(&prefix_mapper);
        let end = end.map(&prefix_mapper);
        let fixed_width = fixed_width_mapper(fixed_width);
        match fixed_width {
            true => KeyRange::new_fixed_width(start, end),
            false => KeyRange::new_variable_width(start, end),
        }
    }

    // pub fn within_end(&self, value: &T) -> bool {
    //     match &self.end {
    //         RangeEnd::SameAsStart => value.starts_with(self.start()),
    //         RangeEnd::Inclusive(e) => value.cmp(e) == Ordering::Less || value.starts_with(e),
    //         RangeEnd::Exclusive(e) => value.cmp(e) == Ordering::Less || *value == self.start,
    //         RangeEnd::Unbounded => true,
    //     }
    // }
    //
    // pub(crate) fn end_value(&self) -> Option<&T> {
    //     match &self.end {
    //         RangeEnd::SameAsStart => Some(self.start()),
    //         RangeEnd::Inclusive(value) => Some(value),
    //         RangeEnd::Exclusive(value) => Some(value),
    //         RangeEnd::Unbounded => None,
    //     }
    // }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStart<T>
where
    T: Ord + Debug,
{
    Inclusive(T),
    Exclusive(T),
}

impl<T> RangeStart<T>
where
    T: Ord + Debug,
{
    pub(crate) fn is_exclusive(&self) -> bool {
        matches!(self, Self::Exclusive(_))
    }

    pub fn map<U: Ord + Debug>(self, mapper: impl FnOnce(T) -> U) -> RangeStart<U> {
        match self {
            Self::Inclusive(end) => RangeStart::Inclusive(mapper(end)),
            Self::Exclusive(end) => RangeStart::Exclusive(mapper(end)),
        }
    }

    pub fn get_value(&self) -> &T {
        match self {
            Self::Inclusive(value) | Self::Exclusive(value) => value,
        }
    }

    pub fn as_bound(&self) -> Bound<&T> {
        match self {
            RangeStart::Inclusive(start) => Bound::Included(start),
            RangeStart::Exclusive(start) => Bound::Excluded(start),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeEnd<T>
where
    T: Ord + Debug,
{
    WithinStartAsPrefix,
    EndPrefixInclusive(T),
    EndPrefixExclusive(T),
    Unbounded,
}

impl<T> RangeEnd<T>
where
    T: Ord + Debug,
{
    pub fn map<U: Ord + Debug>(self, mapper: impl FnOnce(T) -> U) -> RangeEnd<U> {
        match self {
            RangeEnd::WithinStartAsPrefix => RangeEnd::WithinStartAsPrefix,
            RangeEnd::EndPrefixInclusive(end) => RangeEnd::EndPrefixInclusive(mapper(end)),
            RangeEnd::EndPrefixExclusive(end) => RangeEnd::EndPrefixExclusive(mapper(end)),
            RangeEnd::Unbounded => RangeEnd::Unbounded,
        }
    }

    pub fn get_value(&self) -> Option<&T> {
        match self {
            RangeEnd::WithinStartAsPrefix | RangeEnd::Unbounded => None,
            RangeEnd::EndPrefixInclusive(value) | RangeEnd::EndPrefixExclusive(value) => Some(value),
        }
    }
}
