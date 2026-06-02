/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::RangeInclusive;

use itertools::Itertools;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct PrefixID {
    pub(crate) byte: u8,
}

impl PrefixID {
    pub const LENGTH: usize = 1;
    pub const MAX: u8 = u8::MAX;

    pub const fn new(byte: u8) -> Self {
        PrefixID { byte }
    }
    pub const fn to_bytes(&self) -> [u8; PrefixID::LENGTH] {
        [self.byte]
    }
}

macro_rules! make_prefix_enum {
    ($($name:ident => $byte:literal = $hex:literal, $width:expr, $domain:expr);* $(;)?) => {
        // assert that $byte and $hex are the same literal
        $(const _: [(); $byte] = [(); $hex];)*

        #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
        pub enum Prefix {
            $($name = $byte,)*
        }

        impl Prefix {
            pub const ALL: &'static [Prefix] = &[$(Self::$name,)*];

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

            pub fn from_prefix_id(prefix: PrefixID) -> Option<Self> {
                match prefix.byte {
                    $($byte => Some(Self::$name),)*
                    _ => None
                }
            }

            ///
            /// Return true if we expect all keys within this exact prefix to have the same width.
            /// Note: two different prefixes with fixed width are not necessarily the same fixed widths!
            ///
            pub const fn fixed_width_keys(&self) -> bool {
                let width = match self {
                    $(Self::$name => $width,)*
                };
                matches!(width, Width::Fixed)
            }

            /// Return true if this prefix belongs to the schema (types, definitions, type-edges,
            /// type-properties, schema-only indexes). Otherwise the prefix is part of the data
            /// (instances, instance-edges, instance-properties).
            pub const fn is_schema(&self) -> bool {
                let domain = match self {
                    $(Self::$name => $domain,)*
                };
                matches!(domain, MetaDomain::Schema)
            }
        }
    };
}

enum Width {
    Fixed,
    Variable,
}

enum MetaDomain {
    Schema,
    Data,
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

    pub fn schema_byte_ranges() -> Vec<RangeInclusive<u8>> {
        let mut ranges: Vec<RangeInclusive<u8>> = Vec::new();
        let mut current: Option<(u8, u8)> = None;
        for prefix in Self::ALL.iter().copied().sorted_by_key(|p| p.prefix_id()) {
            let byte = prefix.prefix_id().byte;
            if prefix.is_schema() {
                current = Some(match current {
                    Some((start, _)) => (start, byte),
                    None => (byte, byte),
                });
            } else if let Some((start, end)) = current.take() {
                ranges.push(start..=end);
            }
        }
        if let Some((start, end)) = current {
            ranges.push(start..=end);
        }
        ranges
    }
}

make_prefix_enum! {
    // Reserved: 0-9 = 0x00-0x09
    VertexEntityType => 10 = 0x0A, Width::Fixed, MetaDomain::Schema;
    VertexRelationType => 11 = 0x0B, Width::Fixed, MetaDomain::Schema;
    VertexAttributeType => 12 = 0x0C, Width::Fixed, MetaDomain::Schema;
    VertexRoleType => 15 = 0x0F, Width::Fixed, MetaDomain::Schema;
    DefinitionStruct => 20 = 0x14, Width::Fixed, MetaDomain::Schema;
    DefinitionFunction => 21 = 0x15, Width::Fixed, MetaDomain::Schema;

    // All objects are stored consecutively for iteration
    VertexEntity => 30 = 0x1E, Width::Fixed, MetaDomain::Data;
    VertexRelation => 31 = 0x1F, Width::Fixed, MetaDomain::Data;
    VertexAttribute => 32 = 0x20, Width::Variable, MetaDomain::Data;

    EdgeSub => 100 = 0x64, Width::Fixed, MetaDomain::Schema;
    EdgeSubReverse => 101 = 0x65, Width::Fixed, MetaDomain::Schema;
    EdgeOwns => 102 = 0x66, Width::Fixed, MetaDomain::Schema;
    EdgeOwnsReverse => 103 = 0x67, Width::Fixed, MetaDomain::Schema;
    EdgePlays => 104 = 0x68, Width::Fixed, MetaDomain::Schema;
    EdgePlaysReverse => 105 = 0x69, Width::Fixed, MetaDomain::Schema;
    EdgeRelates => 106 = 0x6A, Width::Fixed, MetaDomain::Schema;
    EdgeRelatesReverse => 107 = 0x6B, Width::Fixed, MetaDomain::Schema;

    EdgeHas => 130 = 0x82, Width::Variable, MetaDomain::Data;
    EdgeHasReverse => 131 = 0x83, Width::Variable, MetaDomain::Data;
    EdgeLinks => 132 = 0x84, Width::Fixed, MetaDomain::Data;
    EdgeLinksReverse => 133 = 0x85, Width::Fixed, MetaDomain::Data;
    EdgeLinksIndex => 140 = 0x8C, Width::Fixed, MetaDomain::Data;

    PropertyTypeVertex => 160 = 0xA0, Width::Fixed, MetaDomain::Schema;
    PropertyTypeEdge => 162 = 0xA2, Width::Fixed, MetaDomain::Schema;
    PropertyObjectVertex => 163 = 0xA3, Width::Fixed, MetaDomain::Data;

    IndexLabelToType => 182 = 0xB6, Width::Variable, MetaDomain::Schema;
    IndexNameToDefinitionStruct => 183 = 0xB7, Width::Variable, MetaDomain::Schema;
    IndexNameToDefinitionFunction => 184 = 0xB8, Width::Variable, MetaDomain::Schema;

    IndexValueToStruct => 190 = 0xBE, Width::Variable, MetaDomain::Data;
    // Reserved: 200-255 = 0xC8-0xFF
}
