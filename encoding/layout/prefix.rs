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
use storage::key_value::{StorageKey, StorageKeyReference};
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable};

#[derive(Debug, PartialEq, Eq)]
pub struct PrefixID<'a> {
    bytes: ByteArrayOrRef<'a, { PrefixID::LENGTH }>,
}

impl<'a> PrefixID<'a> {
    pub(crate) const LENGTH: usize = 1;

    pub(crate) const fn new(bytes: ByteArrayOrRef<'a, { PrefixID::LENGTH }>) -> Self {
        PrefixID { bytes: bytes }
    }
}

impl<'a> AsBytes<'a, {PrefixID::LENGTH}> for PrefixID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, {PrefixID::LENGTH}> {
        self.bytes
    }
}

// used as prefix key
impl<'a> Keyable<'a, {PrefixID::LENGTH}> for PrefixID<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        match PrefixType::from_prefix(self) {
            PrefixType::EntityType |
            PrefixType::RelationType |
            PrefixType::AttributeType |
            PrefixType::TypeToLabelIndex |
            PrefixType::LabelToTypeIndex => EncodingKeyspace::Schema.id(),
            PrefixType::Entity => todo!(),
            PrefixType::Attribute => todo!()
        }
    }
}

pub enum PrefixType {
    EntityType,
    RelationType,
    AttributeType,

    TypeToLabelIndex,
    LabelToTypeIndex,

    Entity,
    Attribute,
}

impl PrefixType {
    pub const fn prefix(&self) -> PrefixID {
        let bytes = match self {
            Self::EntityType => &[0],
            Self::RelationType => &[1],
            Self::AttributeType => &[2],
            Self::TypeToLabelIndex => &[20],
            Self::LabelToTypeIndex => &[21],
            Self::Entity => &[100],
            Self::Attribute => &[101],
        };
        PrefixID::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
    }

    // TODO: this is hard to maintain relative to the above - we should convert the pair into a macro or something?
    pub const fn next_prefix(&self) -> PrefixID {
        let bytes = match self {
            Self::EntityType => &[1],
            Self::RelationType => &[2],
            Self::AttributeType => &[3],
            Self::TypeToLabelIndex => &[21],
            Self::LabelToTypeIndex => &[22],
            Self::Entity => &[101],
            Self::Attribute => &[102],
        };
        PrefixID::new(ByteArrayOrRef::Reference(ByteReference::new(bytes)))
    }

    pub fn from_prefix(prefix: &PrefixID) -> PrefixType {
        match prefix.bytes.bytes() {
            [0] => PrefixType::EntityType,
            [1] => PrefixType::RelationType,
            [2] => PrefixType::AttributeType,
            [20] => PrefixType::TypeToLabelIndex,
            [21] => PrefixType::LabelToTypeIndex,

            [100] => PrefixType::Entity,
            [101] => PrefixType::Attribute,

            _ => unreachable!(),
        }
    }
}
