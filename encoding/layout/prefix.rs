/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::util::increment_fixed;

use crate::EncodingKeyspace;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PrefixID {
    pub(crate) bytes: [u8; PrefixID::LENGTH],
}

impl PrefixID {
    pub const VERTEX_ATTRIBUTE_MIN: PrefixID = Self::new([50]);
    pub const VERTEX_ATTRIBUTE_MAX: PrefixID = Self::new([100]);

    pub const LENGTH: usize = 1;

    pub const fn new(bytes: [u8; PrefixID::LENGTH]) -> Self {
        PrefixID { bytes }
    }

    pub const fn bytes(&self) -> [u8; PrefixID::LENGTH] {
        self.bytes
    }

    fn keyspace(&self) -> EncodingKeyspace {
        match Prefix::from_prefix_id(*self) {
            | Prefix::VertexEntityType
            | Prefix::VertexRelationType
            | Prefix::VertexAttributeType
            | Prefix::VertexRoleType
            | Prefix::EdgeSub
            | Prefix::EdgeSubReverse
            | Prefix::EdgeOwns
            | Prefix::EdgeOwnsReverse
            | Prefix::EdgePlays
            | Prefix::EdgePlaysReverse
            | Prefix::EdgeRelates
            | Prefix::EdgeRelatesReverse
            | Prefix::PropertyType
            | Prefix::IndexLabelToType
            | Prefix::PropertyTypeEdge => EncodingKeyspace::Schema,
            Prefix::VertexEntity => todo!(),
            Prefix::VertexRelation => todo!(),
            Prefix::VertexAttributeBoolean => todo!(),
            Prefix::VertexAttributeLong => todo!(),
            Prefix::VertexAttributeDouble => todo!(),
            Prefix::VertexAttributeString => todo!(),
            Prefix::EdgeHas => todo!(),
            Prefix::EdgeHasReverse => todo!(),
            Prefix::EdgeRolePlayer => todo!(),
            Prefix::EdgeRolePlayerReverse => todo!(),
            Prefix::EdgeRolePlayerIndex => todo!(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Prefix {
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

    EdgeSub,
    EdgeSubReverse,
    EdgeOwns,
    EdgeOwnsReverse,
    EdgePlays,
    EdgePlaysReverse,
    EdgeRelates,
    EdgeRelatesReverse,

    EdgeHas,
    EdgeHasReverse,
    EdgeRolePlayer,
    EdgeRolePlayerReverse,
    EdgeRolePlayerIndex,

    PropertyType,
    PropertyTypeEdge,

    IndexLabelToType,
}

macro_rules! prefix_functions {
    ($(
        $name:ident => $bytes:tt
    );*) => {
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

impl Prefix {
    prefix_functions!(
           // Reserved: 0-9

           VertexEntityType => [10];
           VertexRelationType => [11];
           VertexAttributeType => [12];
           VertexRoleType => [15];

           VertexEntity => [30];
           VertexRelation => [31];

           // Reserve: range 50 - 99 to store attribute instances with a value type - see PrefixID::<CONSTANTS>
           VertexAttributeBoolean => [50];
           VertexAttributeLong => [51];
           VertexAttributeDouble => [52];
           VertexAttributeString => [53];

           EdgeSub => [100];
           EdgeSubReverse => [101];
           EdgeOwns => [102];
           EdgeOwnsReverse => [103];
           EdgePlays => [104];
           EdgePlaysReverse => [105];
           EdgeRelates => [106];
           EdgeRelatesReverse => [107];

           EdgeHas => [130];
           EdgeHasReverse => [131];
           EdgeRolePlayer => [132];
           EdgeRolePlayerReverse => [133];
           EdgeRolePlayerIndex => [140];

           PropertyType => [160];
           PropertyTypeEdge => [161];

           IndexLabelToType => [182]

           // Reserved: 200-255
    );
}
