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

use std::{mem, ops::Range};

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, keyspace::keyspace::KeyspaceId};

use crate::{
    graph::{type_::vertex::TypeID, Typed},
    layout::prefix::{PrefixID, PrefixType},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ObjectVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ObjectID::LENGTH;
    const LENGTH_PREFIX_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> ObjectVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        ObjectVertex { bytes: bytes }
    }

    pub fn build_entity(type_id: TypeID, object_id: ObjectID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&PrefixType::VertexEntity.prefix_id().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        array.bytes_mut()[Self::range_object_id()].copy_from_slice(&object_id.bytes());
        ObjectVertex { bytes: ByteArrayOrRef::Array(array) }
    }

    // pub fn build_relation(type_id: &TypeID<'_>, object_id: ObjectID<'_>) -> Self {
    //     let mut array = ByteArray::zeros(Self::LENGTH);
    //     array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::VertexEntity.prefix_id().bytes().bytes());
    //     array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_id.bytes().bytes());
    //     array.bytes_mut()[Self::range_object_id()].copy_from_slice(object_id.bytes().bytes());
    //     ObjectVertex { bytes: ByteArrayOrRef::Array(array) }
    // }

    pub fn build_prefix_prefix(prefix: PrefixID) -> StorageKey<'static, { ObjectVertex::LENGTH_PREFIX_PREFIX }> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_PREFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        StorageKey::new(Self::keyspace_id(), ByteArrayOrRef::Array(array))
    }
    fn build_prefix_type(
        prefix: PrefixID,
        type_id: TypeID,
    ) -> StorageKey<'static, { ObjectVertex::LENGTH_PREFIX_TYPE }> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        StorageKey::new(Self::keyspace_id(), ByteArrayOrRef::Array(array))
    }

    fn keyspace_id() -> KeyspaceId {
        // TODO: partition
        EncodingKeyspace::Data.id()
    }

    pub fn object_id(&self) -> ObjectID {
        ObjectID::new(self.bytes.bytes()[Self::range_object_id()].try_into().unwrap())
    }
    const fn range_object_id() -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + ObjectID::LENGTH
    }

    pub fn into_owned(self) -> ObjectVertex<'static> {
        ObjectVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ObjectID {
    bytes: [u8; { ObjectID::LENGTH }],
}

impl ObjectID {
    const LENGTH: usize = 8;

    fn new(bytes: [u8; { ObjectID::LENGTH }]) -> Self {
        ObjectID { bytes: bytes }
    }

    pub fn build(id: u64) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), Self::LENGTH);
        ObjectID { bytes: id.to_be_bytes() }
    }

    fn bytes(&self) -> [u8; { ObjectID::LENGTH }] {
        self.bytes
    }
}
