/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use crate::{graph::type_::vertex::TypeID, Prefixed};

pub(crate) mod common;
pub mod definition;
pub mod thing;
pub mod type_;

pub trait Typed<'a, const INLINE_SIZE: usize>: Prefixed<'a, INLINE_SIZE> {
    const RANGE_TYPE_ID: Range<usize> = Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeID::LENGTH;

    fn type_id_(&'a self) -> TypeID {
        TypeID::new(self.clone().into_bytes()[Self::RANGE_TYPE_ID].try_into().unwrap())
    }
}
