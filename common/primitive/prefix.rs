/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

pub trait Prefix: Ord + Clone + fmt::Debug {
    fn starts_with(&self, other: &Self) -> bool;

    fn into_starts_with(self, other: Self) -> bool;
}
