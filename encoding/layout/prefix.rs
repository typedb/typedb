/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::util::increment_fixed;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PrefixID {
    pub(crate) bytes: [u8; PrefixID::LENGTH],
}

impl PrefixID {
    pub const LENGTH: usize = 1;

    pub const fn new(bytes: [u8; PrefixID::LENGTH]) -> Self {
        PrefixID { bytes }
    }

    pub const fn bytes(&self) -> [u8; PrefixID::LENGTH] {
        self.bytes
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
    VertexAttributeDecimal,
    VertexAttributeDate,
    VertexAttributeDateTime,
    VertexAttributeDateTimeTZ,
    VertexAttributeDuration,
    VertexAttributeString,
    VertexAttributeStruct,
    _VertexAttributeLast, // marker to indicate reserved range for attribute types

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
    EdgeLinks,
    EdgeLinksReverse,
    EdgeRolePlayerIndex,

    DefinitionStruct,
    DefinitionFunction,

    PropertyTypeVertex,
    PropertyTypeEdge,
    // PropertyDefinitionFunction,
    PropertyObjectVertex,

    IndexLabelToType,
    IndexNameToDefinitionStruct,
    IndexNameToDefinitionFunction,

    IndexValueToStruct,
}

macro_rules! prefix_functions {
    ($(
        $name:ident => $bytes:tt, $fixed_width_keys:literal
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

        ///
        /// Return true if we expect all keys within this exact prefix to have the same width.
        /// Note: two different prefixes with fixed width are not necessarily the same fixed widths!
        ///
       pub const fn fixed_width_keys(&self) -> bool {
            match self {
                $(
                    Self::$name => {$fixed_width_keys}
                )*
            }
        }
   };
}

impl Prefix {
    pub const ATTRIBUTE_MIN: Self = Prefix::VertexAttributeBoolean;
    pub const ATTRIBUTE_MAX: Self = Prefix::_VertexAttributeLast;

    prefix_functions!(
           // Reserved: 0-9
           VertexEntityType => [10], true;
           VertexRelationType => [11], true;
           VertexAttributeType => [12], true;
           VertexRoleType => [15], true;
           DefinitionStruct => [20], true;
           DefinitionFunction => [21], true;

           // All objects are stored consecutively for iteration
           VertexEntity => [30], true;
           VertexRelation => [31], true;

           // Reserve: range 50 - 99 to store attribute instances with a value type - see PrefixID::<CONSTANTS>
           VertexAttributeBoolean => [50], true;
           VertexAttributeLong => [51], true;
           VertexAttributeDouble => [52], true;
           VertexAttributeDecimal => [53], true;
           VertexAttributeDate => [54], true;
           VertexAttributeDateTime => [55], true;
           VertexAttributeDateTimeTZ => [56], true;
           VertexAttributeDuration => [57], true;
           VertexAttributeString => [58], true;
           VertexAttributeStruct => [90], true;
           _VertexAttributeLast => [99], true;

           EdgeSub => [100], true;
           EdgeSubReverse => [101], true;
           EdgeOwns => [102], true;
           EdgeOwnsReverse => [103], true;
           EdgePlays => [104], true;
           EdgePlaysReverse => [105], true;
           EdgeRelates => [106], true;
           EdgeRelatesReverse => [107], true;

           EdgeHas => [130], false;
           EdgeHasReverse => [131], false;
           EdgeLinks => [132], true;
           EdgeLinksReverse => [133], true;
           EdgeRolePlayerIndex => [140], true;

           PropertyTypeVertex => [160], true;
           PropertyTypeEdge => [162], true;
           PropertyObjectVertex => [163], true;

           IndexLabelToType => [182], false;
           IndexNameToDefinitionStruct => [183], false;
           IndexNameToDefinitionFunction => [184], false;

           IndexValueToStruct => [190], false
           // Reserved: 200-255
    );
}
