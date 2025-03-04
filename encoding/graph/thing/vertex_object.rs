/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, mem, ops::Range};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKeyReference, keyspace::KeyspaceSet};

use crate::{
    graph::{
        thing::ThingVertex,
        type_::vertex::{TypeID, TypeVertex},
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct ObjectVertex {
    prefix: Prefix,
    type_id: TypeID,
    object_id: ObjectID,
}

impl ObjectVertex {
    pub const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    pub const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ObjectID::LENGTH;
    pub const MIN: Self = Self::MIN_ENTITY;
    pub const MIN_ENTITY: Self = Self::build_entity(TypeID::MIN, ObjectID::MIN);
    pub const MIN_RELATION: Self = Self::build_relation(TypeID::MIN, ObjectID::MIN);

    pub const fn build_entity(type_id: TypeID, object_id: ObjectID) -> Self {
        Self { prefix: Prefix::VertexEntity, type_id, object_id }
    }

    pub const fn build_relation(type_id: TypeID, object_id: ObjectID) -> Self {
        Self { prefix: Prefix::VertexRelation, type_id, object_id }
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        let prefix = bytes[0];
        if prefix != Prefix::VertexEntity.prefix_id().byte && prefix != Prefix::VertexRelation.prefix_id().byte {
            return None;
        }

        // all byte patterns beyond the prefix are valid for object vertices
        Some(Self::decode(bytes))
    }

    pub fn is_entity_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        storage_key.keyspace_id() == Self::KEYSPACE.id()
            && storage_key.bytes().len() == Self::LENGTH
            && storage_key.bytes()[Self::INDEX_PREFIX] == Prefix::VertexEntity.prefix_id().byte
    }

    pub fn is_relation_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        storage_key.keyspace_id() == Self::KEYSPACE.id()
            && storage_key.bytes().len() == Self::LENGTH
            && storage_key.bytes()[Self::INDEX_PREFIX] == Prefix::VertexRelation.prefix_id().byte
    }

    pub(crate) fn write_prefix_from_type_vertex(bytes: &mut [u8], type_vertex: TypeVertex) -> usize {
        Self::write_prefix_type(bytes, Self::prefix_for_type(type_vertex.prefix()), type_vertex.type_id_())
    }

    pub(crate) const fn prefix_for_type(prefix: Prefix) -> Prefix {
        match prefix {
            Prefix::VertexEntityType => Prefix::VertexEntity,
            Prefix::VertexRelationType => Prefix::VertexRelation,
            _ => unreachable!(),
        }
    }

    pub fn object_id(&self) -> ObjectID {
        self.object_id
    }

    pub(crate) fn len(&self) -> usize {
        Self::LENGTH
    }

    const fn range_object_id() -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + ObjectID::LENGTH
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ObjectVertex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = [0; BUFFER_KEY_INLINE];
        bytes[0] = self.prefix.prefix_id().byte;
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&self.type_id.to_bytes());
        bytes[Self::range_object_id()].copy_from_slice(&self.object_id.to_bytes());
        Bytes::Array(ByteArray::inline(bytes, Self::LENGTH))
    }
}

impl Keyable<BUFFER_KEY_INLINE> for ObjectVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ObjectVertex {}

impl Typed<BUFFER_KEY_INLINE> for ObjectVertex {
    fn type_id_(&self) -> TypeID {
        self.type_id
    }
}

impl ThingVertex for ObjectVertex {
    const FIXED_WIDTH_ENCODING: bool = true;

    fn decode(bytes: &[u8]) -> ObjectVertex {
        debug_assert_eq!(bytes.len(), Self::LENGTH);
        let prefix = Prefix::from_prefix_id(PrefixID { byte: bytes[0] });
        let type_id = TypeID::decode(bytes[Self::RANGE_TYPE_ID].try_into().unwrap());
        let object_id = ObjectID::decode(bytes[Self::range_object_id()].try_into().unwrap());
        Self { prefix, type_id, object_id }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectID {
    value: u64,
}

impl ObjectID {
    pub(crate) const LENGTH: usize = 8;
    pub const MIN: Self = Self::new_const(0);

    pub fn new(id: u64) -> Self {
        // TODO: mem::size_of_val isn't const yet
        debug_assert_eq!(mem::size_of_val(&id), Self::LENGTH);
        Self::new_const(id)
    }

    const fn new_const(id: u64) -> Self {
        ObjectID { value: id }
    }

    pub fn decode(bytes: [u8; ObjectID::LENGTH]) -> Self {
        ObjectID { value: u64::from_be_bytes(bytes) }
    }

    pub fn to_bytes(self) -> [u8; ObjectID::LENGTH] {
        self.value.to_be_bytes()
    }

    pub fn as_u64(&self) -> u64 {
        self.value
    }
}

impl fmt::Display for ObjectID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(&self.to_bytes()))
    }
}
