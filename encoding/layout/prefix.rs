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

use bytes::increment_fixed;
use storage::keyspace::keyspace::KeyspaceId;

use crate::EncodingKeyspace;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PrefixID {
    pub(crate) bytes: [u8; PrefixID::LENGTH],
}

impl PrefixID {
    pub const VERTEX_ATTRIBUTE_MIN: PrefixID = Self::new([50]);
    pub const VERTEX_ATTRIBUTE_MAX: PrefixID = Self::new([100]);

    pub(crate) const LENGTH: usize = 1;

    pub(crate) const fn new(bytes: [u8; PrefixID::LENGTH]) -> Self {
        PrefixID { bytes }
    }

    pub(crate) const fn bytes(&self) -> [u8; PrefixID::LENGTH] {
        self.bytes
    }

    fn keyspace_id(&self) -> KeyspaceId {
        match PrefixType::from_prefix_id(*self) {
            PrefixType::VertexEntityType
            | PrefixType::VertexRelationType
            | PrefixType::VertexAttributeType
            | PrefixType::VertexRoleType
            | PrefixType::PropertyType
            | PrefixType::IndexLabelToType
            | PrefixType::PropertyTypeEdge => EncodingKeyspace::Schema.id(),
            PrefixType::VertexEntity => todo!(),
            PrefixType::VertexRelation => todo!(),
            PrefixType::VertexAttributeBoolean => todo!(),
            PrefixType::VertexAttributeLong => todo!(),
            PrefixType::VertexAttributeDouble => todo!(),
            PrefixType::VertexAttributeString => todo!(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PrefixType {
    VertexEntityType,
    VertexRelationType,
    VertexAttributeType,
    VertexRoleType,

    VertexEntity,
    VertexRelation,

    VertexAttributeBoolean,
    VertexAttributeLong,
    VertexAttributeDouble,
    VertexAttributeString,

    PropertyType,
    PropertyTypeEdge,

    IndexLabelToType,
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
            PrefixID::new(*bytes)
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
            PrefixID::new(*bytes)
        }

        pub fn from_prefix_id(prefix: PrefixID) -> Self {
            match prefix.bytes() {
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
           VertexEntityType => [10],
           VertexRelationType => [11],
           VertexAttributeType => [12],
           VertexRoleType => [20],

           VertexEntity => [40],
           VertexRelation => [41],

           // We reserve the range 50 - 99 (inclusive) to store attribute instances with a value type - see PrefixID::<CONSTANTS>
           VertexAttributeBoolean => [50],
           VertexAttributeLong => [51],
           VertexAttributeDouble => [52],
           VertexAttributeString => [53],

           PropertyType => [100],
           PropertyTypeEdge => [101],
           IndexLabelToType => [102]
    );
}
