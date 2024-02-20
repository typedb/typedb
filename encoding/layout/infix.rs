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

use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;


pub(crate) struct Infix<'a> {
    bytes: ByteArrayOrRef<'a, { Infix::LENGTH }>,
}

impl<'a> Infix<'a> {
    pub(crate) const LENGTH: usize = 1;

    const fn new(bytes: ByteArrayOrRef<'a, { Infix::LENGTH }>) -> Self {
        Infix {
            bytes: bytes
        }
    }

    pub(crate) fn bytes(&self) -> &ByteArrayOrRef<'a, { Infix::LENGTH }> {
        &self.bytes
    }
}

pub(crate) enum InfixType {
    OwnsForward,
    OwnsBackward,

    HasForward,
    HasBackward,
}

impl InfixType {
    pub(crate) const fn as_infix(&self) -> Infix<'static> {
        let bytes = match self {
            InfixType::OwnsForward => &[6],
            InfixType::OwnsBackward => &[7],

            InfixType::HasForward => &[50],
            InfixType::HasBackward => &[51],
        };
        Infix::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
    }
}
