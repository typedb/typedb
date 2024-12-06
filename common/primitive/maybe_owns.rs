/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Deref;

#[derive(Debug)]
pub enum MaybeOwns<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<T> Deref for MaybeOwns<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            MaybeOwns::Owned(owned) => owned,
            MaybeOwns::Borrowed(borrowed) => borrowed,
        }
    }
}

impl<T: PartialEq> PartialEq<T> for MaybeOwns<'_, T> {
    fn eq(&self, other: &T) -> bool {
        self.deref().eq(other)
    }
}

impl<T: PartialEq> PartialEq for MaybeOwns<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<T: Eq> Eq for MaybeOwns<'_, T> {}

impl<'a, T> IntoIterator for &'a MaybeOwns<'_, T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&**self).into_iter()
    }
}
