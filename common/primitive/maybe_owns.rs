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
use std::ops::Deref;

pub enum MaybeOwns<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> MaybeOwns<'a, T> {

    pub fn owned(t: T) -> Self {
        Self::Owned(t)
    }

    pub fn borrowed(t: &'a T) -> Self {
        Self::Borrowed(t)
    }
}

impl<T> Deref for MaybeOwns<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            MaybeOwns::Owned(owned) => &owned,
            MaybeOwns::Borrowed(ref borrowed) => &borrowed,
        }
    }
}

