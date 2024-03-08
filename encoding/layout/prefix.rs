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
use bytes::increment_fixed;
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixID<'a> {
    bytes: ByteArrayOrRef<'a, { PrefixID::LENGTH }>,
}

impl<'a> PrefixID<'a> {
    pub(crate) const LENGTH: usize = 1;

    pub(crate) const fn new(bytes: ByteArrayOrRef<'a, { PrefixID::LENGTH }>) -> Self {
        PrefixID { bytes: bytes }
    }
}

impl<'a> AsBytes<'a, { PrefixID::LENGTH }> for PrefixID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { PrefixID::LENGTH }> {
        self.bytes
    }
}

// used as prefix key
impl<'a> Keyable<'a, { PrefixID::LENGTH }> for PrefixID<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        match PrefixType::from_prefix_id(self.clone()) {
            PrefixType::VertexEntityType |
            PrefixType::VertexRelationType |
            PrefixType::VertexAttributeType |
            PrefixType::PropertyTypeToLabel |
            PrefixType::PropertyLabelToType => EncodingKeyspace::Schema.id(),
            PrefixType::VertexEntity => todo!(),
            PrefixType::VertexAttribute => todo!()
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PrefixType {
    VertexEntityType,
    VertexRelationType,
    VertexAttributeType,

    VertexEntity,
    VertexAttribute,

    PropertyTypeToLabel,
    PropertyLabelToType,
}

macro_rules! prefix_functions {
    ($(
        $name:ident => $bytes:tt
    ),*) => {
        pub const fn prefix_id(&self) -> PrefixID {
            let bytes = match self {
                $(
                    Self::$name => {&$bytes}
                )*
            };
            PrefixID::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
        }

        pub const fn successor_prefix_id(&self) -> PrefixID {
            let bytes = match self {
                $(
                    Self::$name => {
                        const SUCCESSOR: [u8; PrefixID::LENGTH] = increment_fixed($bytes);
                        &SUCCESSOR
                    }
                )*
            };
            PrefixID::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
        }

        pub fn from_prefix_id(prefix: PrefixID) -> Self {
            match prefix.bytes.bytes() {
                $(
                    $bytes => {Self::$name}
                )*
                _ => unreachable!(),
            }
       }
   };
}

impl PrefixType {
    prefix_functions!(
           VertexEntityType => [20],
           VertexRelationType => [21],
           VertexAttributeType => [22],

           VertexEntity => [60],
           VertexAttribute => [61],

           PropertyTypeToLabel => [100],
           PropertyLabelToType => [101]
    );
}
