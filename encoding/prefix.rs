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

pub const PREFIX_SIZE: usize = 1;

pub enum Prefix {
    EntityType,
    RelationType,
    AttributeType,

    TypeLabelIndex,
    LabelTypeIndex,

    Entity,
    Attribute,
}

#[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PrefixID {
    id: [u8; PREFIX_SIZE],
}

impl Prefix {
    pub const fn as_bytes(&self) -> [u8; PREFIX_SIZE] {
        match self {
            Prefix::EntityType => [0],
            Prefix::RelationType => [1],
            Prefix::AttributeType => [2],
            Prefix::TypeLabelIndex => [20],
            Prefix::LabelTypeIndex => [21],
            Prefix::Entity => [100],
            Prefix::Attribute => [101],
        }
    }

    // TODO: this is hard to maintain relative to the above - we should convert the pair into a macro
    pub const fn as_bytes_next(&self) -> [u8; PREFIX_SIZE] {
        match self {
            Prefix::EntityType => [1],
            Prefix::RelationType => [2],
            Prefix::AttributeType => [3],
            Prefix::TypeLabelIndex => [21],
            Prefix::LabelTypeIndex => [22],
            Prefix::Entity => [101],
            Prefix::Attribute => [102],
        }
    }

    pub const fn as_id(&self) -> PrefixID {
        PrefixID { id: self.as_bytes() }
    }
}

