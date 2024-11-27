/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, mem, ops::Range};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::{StorageKey, StorageKeyReference},
    keyspace::KeyspaceSet,
};

use crate::{
    graph::{
        thing::{ThingVertex, THING_VERTEX_LENGTH_PREFIX_TYPE},
        type_::vertex::{TypeID, TypeVertex},
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct ObjectVertex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ObjectVertex<'a> {
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ObjectID::LENGTH;

    pub fn build_entity(type_id: TypeID, object_id: ObjectID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::RANGE_PREFIX].copy_from_slice(&Prefix::VertexEntity.prefix_id().bytes());
        array[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        array[Self::range_object_id()].copy_from_slice(&object_id.bytes());
        ObjectVertex { bytes: Bytes::Array(array) }
    }

    pub fn build_relation(type_id: TypeID, object_id: ObjectID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::RANGE_PREFIX].copy_from_slice(&Prefix::VertexRelation.prefix_id().bytes());
        array[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        array[Self::range_object_id()].copy_from_slice(&object_id.bytes());
        ObjectVertex { bytes: Bytes::Array(array) }
    }

    pub fn try_from_bytes(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        let prefix = &bytes[..1];
        if prefix != Prefix::VertexEntity.prefix_id().bytes() && prefix != Prefix::VertexRelation.prefix_id().bytes() {
            return None;
        }
        // all byte patterns beyond the prefix are valid for object vertices
        Some(Self::new(Bytes::reference(bytes)))
    }

    pub fn is_entity_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        storage_key.keyspace_id() == Self::KEYSPACE.id()
            && storage_key.bytes().len() == Self::LENGTH
            && storage_key.bytes()[Self::RANGE_PREFIX] == Prefix::VertexEntity.prefix_id().bytes
    }

    pub fn is_relation_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        storage_key.keyspace_id() == Self::KEYSPACE.id()
            && storage_key.bytes().len() == Self::LENGTH
            && storage_key.bytes()[Self::RANGE_PREFIX] == Prefix::VertexRelation.prefix_id().bytes
    }

    pub(crate) fn build_prefix_from_type_vertex(
        type_vertex: TypeVertex,
    ) -> StorageKey<'static, { THING_VERTEX_LENGTH_PREFIX_TYPE }> {
        let prefix = match type_vertex.prefix() {
            Prefix::VertexEntityType => Prefix::VertexEntity,
            Prefix::VertexRelationType => Prefix::VertexRelation,
            _ => unreachable!(),
        };
        Self::build_prefix_type(prefix, type_vertex.type_id_())
    }

    pub fn object_id(&self) -> ObjectID {
        ObjectID::new(self.bytes[Self::range_object_id()].try_into().unwrap())
    }

    pub(crate) fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn as_reference<'this: 'a>(&'this self) -> ObjectVertex<'this> {
        Self::new(Bytes::Reference(&self.bytes))
    }

    const fn range_object_id() -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + ObjectID::LENGTH
    }

    pub fn into_owned(self) -> ObjectVertex<'static> {
        ObjectVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

impl<'a> ThingVertex<'a> for ObjectVertex<'a> {
    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> ObjectVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        ObjectVertex { bytes }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ObjectID {
    bytes: [u8; ObjectID::LENGTH],
}

impl ObjectID {
    const LENGTH: usize = 8;

    fn new(bytes: [u8; ObjectID::LENGTH]) -> Self {
        ObjectID { bytes }
    }

    pub fn build(id: u64) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), Self::LENGTH);
        ObjectID { bytes: id.to_be_bytes() }
    }

    fn bytes(&self) -> [u8; ObjectID::LENGTH] {
        self.bytes
    }

    pub fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.bytes)
    }
}

impl fmt::Display for ObjectID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(&self.bytes()))
    }
}
