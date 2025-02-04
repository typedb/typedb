/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PrefixID {
    pub(crate) byte: u8,
}

impl PrefixID {
    pub const LENGTH: usize = 1;
    pub const fn new(byte: u8) -> Self {
        PrefixID { byte }
    }
    pub const fn to_bytes(&self) -> [u8; PrefixID::LENGTH] {
        [self.byte]
    }
}

macro_rules! make_prefix_enum {
    ($($name:ident => $byte:literal, $fixed_width_keys:literal);*) => {
        #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
        pub enum Prefix {
            $($name = $byte,)*
        }

        impl Prefix {
            pub const fn prefix_id(&self) -> PrefixID {
                match self {
                    $(Self::$name => PrefixID::new($byte),)*
                }
            }

            pub const fn successor_prefix_id(&self) -> PrefixID {
                match self {
                    $(Self::$name => PrefixID::new($byte + 1),)*
                }
            }

            pub fn from_prefix_id(prefix: PrefixID) -> Self {
                match prefix.byte {
                    $($byte => Self::$name,)*
                    _ => unreachable!(),
                }
           }

            ///
            /// Return true if we expect all keys within this exact prefix to have the same width.
            /// Note: two different prefixes with fixed width are not necessarily the same fixed widths!
            ///
           pub const fn fixed_width_keys(&self) -> bool {
                match self {
                    $(Self::$name => $fixed_width_keys,)*
                }
            }
        }
    };
}

impl Prefix {
    pub fn max_object_type_prefix() -> Prefix {
        if Prefix::VertexEntityType.prefix_id().byte < Prefix::VertexRelationType.prefix_id().byte {
            Prefix::VertexEntityType
        } else {
            Prefix::VertexRelationType
        }
    }

    pub fn min_object_type_prefix() -> Prefix {
        if Prefix::VertexEntityType.prefix_id().byte < Prefix::VertexRelationType.prefix_id().byte {
            Prefix::VertexRelationType
        } else {
            Prefix::VertexRelationType
        }
    }

    pub fn object_type_range_inclusive() -> (Prefix, Prefix) {
        (Self::min_object_type_prefix(), Self::max_object_type_prefix())
    }
}

make_prefix_enum! {
    // Reserved: 0-9
    VertexEntityType => 10, true;
    VertexRelationType => 11, true;
    VertexAttributeType => 12, true;
    VertexRoleType => 15, true;
    DefinitionStruct => 20, true;
    DefinitionFunction => 21, true;

    // All objects are stored consecutively for iteration
    VertexEntity => 30, true;
    VertexRelation => 31, true;
    VertexAttribute => 32, false;

    EdgeSub => 100, true;
    EdgeSubReverse => 101, true;
    EdgeOwns => 102, true;
    EdgeOwnsReverse => 103, true;
    EdgePlays => 104, true;
    EdgePlaysReverse => 105, true;
    EdgeRelates => 106, true;
    EdgeRelatesReverse => 107, true;

    EdgeHas => 130, false;
    EdgeHasReverse => 131, false;
    EdgeLinks => 132, true;
    EdgeLinksReverse => 133, true;
    EdgeLinksIndex => 140, true;

    PropertyTypeVertex => 160, true;
    PropertyTypeEdge => 162, true;
    PropertyObjectVertex => 163, true;

    IndexLabelToType => 182, false;
    IndexNameToDefinitionStruct => 183, false;
    IndexNameToDefinitionFunction => 184, false;

    IndexValueToStruct => 190, false
    // Reserved: 200-255
}
