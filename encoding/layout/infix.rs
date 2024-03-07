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
use crate::AsBytes;


pub(crate) struct InfixID<'a> {
    bytes: ByteArrayOrRef<'a, { InfixID::LENGTH }>,
}

impl<'a> InfixID<'a> {
    pub(crate) const LENGTH: usize = 1;

    pub const fn new(bytes: ByteArrayOrRef<'a, { InfixID::LENGTH }>) -> Self {
        InfixID { bytes: bytes }
    }
}

impl<'a> AsBytes<'a, { InfixID::LENGTH }> for InfixID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { InfixID::LENGTH }> {
        self.bytes
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum InfixType {
    SubForward,
    SubBackward,
    OwnsForward,
    OwnsBackward,
    PlaysForward,
    PlaysBackward,
    RelatesForward,
    RelatesBackward,

    HasForward,
    HasBackward,
}

macro_rules! infix_functions {
    ($(
        $name:ident => $bytes:tt
    ),*) => {
        pub(crate) const fn infix_id(&self) -> InfixID {
            let bytes = match self {
                $(
                    Self::$name => {&$bytes}
                )*
            };
            InfixID::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
        }

        pub(crate) fn from_infix_id(infix_id: InfixID) -> Self {
            match infix_id.bytes.bytes() {
                $(
                    $bytes => {Self::$name}
                )*
                _ => unreachable!(),
            }
       }
   };
}

impl InfixType {
    infix_functions!(
        SubForward => [20],
        SubBackward => [21],
        OwnsForward => [22],
        OwnsBackward => [23],
        PlaysForward => [24],
        PlaysBackward => [25],
        RelatesForward => [26],
        RelatesBackward => [27],

        HasForward => [50],
        HasBackward => [51]
    );
}
