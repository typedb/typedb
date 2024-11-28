/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Debug;

use primitive::prefix::Prefix;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange<T: Prefix> {
    // inclusive
    start: RangeStart<T>,
    end: RangeEnd<T>,
    fixed_width_keys: bool,
}

impl<T: Prefix> KeyRange<T> {
    pub fn new(start: RangeStart<T>, end: RangeEnd<T>, fixed_width_keys: bool) -> Self {
        Self { start, end, fixed_width_keys }
    }

    pub fn new_variable_width(start: RangeStart<T>, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: false }
    }

    pub fn new_fixed_width(start: RangeStart<T>, end: RangeEnd<T>) -> Self {
        Self { start, end, fixed_width_keys: true }
    }

    pub fn new_unbounded(start: RangeStart<T>) -> Self {
        Self { start, end: RangeEnd::Unbounded, fixed_width_keys: false }
    }

    pub fn new_within(prefix: T, fixed_width_keys: bool) -> Self {
        Self { start: RangeStart::Inclusive(prefix), end: RangeEnd::WithinStartAsPrefix, fixed_width_keys }
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

    pub fn map<'a, V: Prefix>(
        &'a self,
        prefix_mapper: impl Fn(&'a T) -> V,
        fixed_width_mapper: impl Fn(bool) -> bool,
    ) -> KeyRange<V> {
        let start = (&self.start).map(&prefix_mapper);
        let end = (&self.end).map(&prefix_mapper);
        let fixed_width = fixed_width_mapper(self.fixed_width_keys);
        match fixed_width {
            true => KeyRange::new_fixed_width(start, end),
            false => KeyRange::new_variable_width(start, end),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStart<T>
where
    T: Ord + Debug,
{
    Inclusive(T),
    ExcludeFirstWithPrefix(T),
    ExcludePrefix(T),
}

impl<T> RangeStart<T>
where
    T: Ord + Debug,
{
    pub fn map<'a: 'b, 'b, U: Ord + Debug + 'b>(&'a self, mapper: impl FnOnce(&'a T) -> U) -> RangeStart<U> {
        match self {
            Self::Inclusive(end) => RangeStart::Inclusive(mapper(end)),
            Self::ExcludeFirstWithPrefix(end) => RangeStart::ExcludeFirstWithPrefix(mapper(end)),
            Self::ExcludePrefix(end) => RangeStart::ExcludePrefix(mapper(end)),
        }
    }

    pub fn get_value(&self) -> &T {
        match self {
            Self::Inclusive(value) | Self::ExcludeFirstWithPrefix(value) | Self::ExcludePrefix(value) => value,
        }
    }
    //
    // pub fn as_bound(&self) -> Bound<&T> {
    //     match self {
    //         RangeStart::Inclusive(start) => Bound::Included(start),
    //         RangeStart::Exclusive(start) => Bound::Excluded(start),
    //     }
    // }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeEnd<T>
where
    T: Ord + Debug,
{
    // WARNING: only to be used with RangeStart::Inclusive
    WithinStartAsPrefix,

    EndPrefixInclusive(T),
    EndPrefixExclusive(T),
    Unbounded,
}

impl<T> RangeEnd<T>
where
    T: Ord + Debug,
{
    pub fn map<'a: 'b, 'b, U: Ord + Debug + 'b>(&'a self, mapper: impl FnOnce(&'a T) -> U) -> RangeEnd<U> {
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
