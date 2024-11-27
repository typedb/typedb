/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Bound;

pub mod either;
pub mod maybe_owns;
pub mod prefix;


// a type that implements RangeBounds<T>
pub type Bounds<T> = (Bound<T>, Bound<T>);
