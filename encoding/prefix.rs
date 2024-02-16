/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use struct_deser_derive::StructDeser;
use crate::{EncodingKeyspace, Serialisable, SerialisableKeyFixed};

pub const PREFIX_SIZE: usize = 1;

#[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PrefixID {
    id: [u8; PREFIX_SIZE],
}

impl SerialisableKeyFixed for PrefixID {
    fn key_section_id(&self) -> u8 {
        match Prefix::from_prefix_id(self) {
            Prefix::EntityType |
            Prefix::RelationType |
            Prefix::AttributeType |
            Prefix::TypeLabelIndex |
            Prefix::LabelTypeIndex  => EncodingKeyspace::Schema.id(),
            Prefix::Entity => todo!(),
            Prefix::Attribute => todo!()
        }
    }
}

pub enum Prefix {
    EntityType,
    RelationType,
    AttributeType,

    TypeLabelIndex,
    LabelTypeIndex,

    Entity,
    Attribute,
}

impl Prefix {
    pub const fn type_id(&self) -> PrefixID {
        match self {
            Prefix::EntityType => PrefixID { id: [0] },
            Prefix::RelationType => PrefixID { id: [1] },
            Prefix::AttributeType => PrefixID { id: [2] },
            Prefix::TypeLabelIndex => PrefixID { id: [20] },
            Prefix::LabelTypeIndex => PrefixID { id: [21] },
            Prefix::Entity => PrefixID { id: [100] },
            Prefix::Attribute => PrefixID { id: [101] },
        }
    }

    // TODO: this is hard to maintain relative to the above - we should convert the pair into a macro
    pub const fn next_prefix_id(&self) -> PrefixID {
        match self {
            Prefix::EntityType => PrefixID { id: [1] },
            Prefix::RelationType => PrefixID { id: [2] },
            Prefix::AttributeType => PrefixID { id: [3] },
            Prefix::TypeLabelIndex => PrefixID { id: [21] },
            Prefix::LabelTypeIndex => PrefixID { id: [22] },
            Prefix::Entity => PrefixID { id: [101] },
            Prefix::Attribute => PrefixID { id: [102] },
        }
    }

    pub const fn from_prefix_id(prefix_id: &PrefixID) -> Prefix {
        match prefix_id.id {
            [0] => Prefix::EntityType,
            [1] => Prefix::RelationType,
            [2] => Prefix::AttributeType,
            [20] => Prefix::TypeLabelIndex,
            [21] => Prefix::LabelTypeIndex,

            [100] => Prefix::Entity,
            [101] => Prefix::Attribute,

            _ => unreachable!(),
        }
    }
}
