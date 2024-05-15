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

impl<'a, T: PartialEq> PartialEq<T> for MaybeOwns<'a, T> {
    fn eq(&self, other: &T) -> bool {
        self.deref().eq(other)
    }
}

impl<'a, T: PartialEq> PartialEq for MaybeOwns<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<'a, T: Eq> Eq for MaybeOwns<'a, T> {}

impl<'a, 'b, T> IntoIterator for &'b MaybeOwns<'a, T> where &'b T: IntoIterator {
    type Item = <&'b T as IntoIterator>::Item;
    type IntoIter = <&'b T as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&**self).into_iter()
    }
}
