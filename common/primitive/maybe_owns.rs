/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Deref;

#[derive(Debug)]
pub enum MaybeOwns<'a, T>
where
    T: Eq,
{
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T: Eq> MaybeOwns<'a, T> {
    pub fn owned(t: T) -> Self {
        Self::Owned(t)
    }

    pub fn borrowed(t: &'a T) -> Self {
        Self::Borrowed(t)
    }
}

impl<T: Eq> Deref for MaybeOwns<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            MaybeOwns::Owned(owned) => owned,
            MaybeOwns::Borrowed(borrowed) => borrowed,
        }
    }
}

impl<'a, T: Eq> PartialEq<Self> for MaybeOwns<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<'a, T: Eq> Eq for MaybeOwns<'a, T> {}
