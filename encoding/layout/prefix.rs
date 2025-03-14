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
    ($($name:ident => $byte:literal = $hex:literal, $fixed_width_keys:literal);*) => {
        // assert that $byte and $hex are the same literal
        $(const _: [(); $byte] = [(); $hex];)*

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
            Prefix::VertexEntityType
        }
    }
}

make_prefix_enum! {
    // Reserved: 0-9 = 0x00-0x09
    VertexEntityType => 10 = 0x0A, true;
    VertexRelationType => 11 = 0x0B, true;
    VertexAttributeType => 12 = 0x0C, true;
    VertexRoleType => 15 = 0x0F, true;
    DefinitionStruct => 20 = 0x14, true;
    DefinitionFunction => 21 = 0x15, true;

    // All objects are stored consecutively for iteration
    VertexEntity => 30 = 0x1E, true;
    VertexRelation => 31 = 0x1F, true;
    VertexAttribute => 32 = 0x20, false;

    EdgeSub => 100 = 0x64, true;
    EdgeSubReverse => 101 = 0x65, true;
    EdgeOwns => 102 = 0x66, true;
    EdgeOwnsReverse => 103 = 0x67, true;
    EdgePlays => 104 = 0x68, true;
    EdgePlaysReverse => 105 = 0x69, true;
    EdgeRelates => 106 = 0x6A, true;
    EdgeRelatesReverse => 107 = 0x6B, true;

    EdgeHas => 130 = 0x82, false;
    EdgeHasReverse => 131 = 0x83, false;
    EdgeLinks => 132 = 0x84, true;
    EdgeLinksReverse => 133 = 0x85, true;
    EdgeLinksIndex => 140 = 0x8C, true;

    PropertyTypeVertex => 160 = 0xA0, true;
    PropertyTypeEdge => 162 = 0xA2, true;
    PropertyObjectVertex => 163 = 0xA3, true;

    IndexLabelToType => 182 = 0xB6, false;
    IndexNameToDefinitionStruct => 183 = 0xB7, false;
    IndexNameToDefinitionFunction => 184 = 0xB8, false;

    IndexValueToStruct => 190 = 0xBE, false
    // Reserved: 200-255 = 0xC8-0xFF
}
