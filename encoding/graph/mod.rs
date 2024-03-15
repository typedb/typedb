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

use std::ops::Range;

use crate::{graph::type_::vertex::TypeID, Prefixed};

pub mod thing;
pub mod type_;

pub trait Typed<'a, const INLINE_SIZE: usize>: Prefixed<'a, INLINE_SIZE> {
    const RANGE_TYPE_ID: Range<usize> = Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeID::LENGTH;

    fn type_id(&'a self) -> TypeID {
        TypeID::new(self.bytes().bytes()[Self::RANGE_TYPE_ID].try_into().unwrap())
    }
}
