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

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, keyspace::keyspace::KeyspaceId};

use crate::{
    graph::Typed,
    layout::prefix::{PrefixID, PrefixType},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

// TODO: we could make all Type constructs contain plain byte arrays, since they will always be 64 bytes (BUFFER_KEY_INLINE), then make Types all Copy
//       However, we should benchmark this first, since 64 bytes may be better off referenced

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_vertex_constructors {
    ($new_name:ident, $build_name:ident, $build_name_prefix:ident, $is_name:ident, PrefixType::$prefix:ident) => {
        pub fn $new_name(bytes: ByteArrayOrRef<'_, BUFFER_KEY_INLINE>) -> TypeVertex<'_> {
            let vertex = TypeVertex::new(bytes);
            debug_assert_eq!(vertex.prefix(), PrefixType::$prefix);
            vertex
        }

        pub fn $build_name(type_id: TypeID) -> TypeVertex<'static> {
            TypeVertex::build(PrefixType::$prefix.prefix_id(), type_id)
        }

        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        pub const fn $build_name_prefix() -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
            const BYTES: [u8; TypeVertex::LENGTH_PREFIX] = PrefixType::$prefix.prefix_id().bytes();
            StorageKey::new_ref(TypeVertex::keyspace_id(), ByteReference::new(&BYTES))
        }

        pub fn $is_name(bytes: ByteArrayOrRef<'_, BUFFER_KEY_INLINE>) -> bool {
            bytes.length() == TypeVertex::LENGTH && TypeVertex::new(bytes).prefix() == PrefixType::$prefix
        }
    };
}

type_vertex_constructors!(
    new_vertex_entity_type,
    build_vertex_entity_type,
    build_vertex_entity_type_prefix,
    is_vertex_entity_type,
    PrefixType::VertexEntityType
);
type_vertex_constructors!(
    new_vertex_relation_type,
    build_vertex_relation_type,
    build_vertex_relation_type_prefix,
    is_vertex_relation_type,
    PrefixType::VertexRelationType
);
type_vertex_constructors!(
    new_vertex_attribute_type,
    build_vertex_attribute_type,
    build_vertex_attribute_type_prefix,
    is_vertex_attribute_type,
    PrefixType::VertexAttributeType
);

impl<'a> TypeVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> TypeVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeVertex { bytes }
    }

    fn build(prefix: PrefixID, type_id: TypeID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        TypeVertex { bytes: ByteArrayOrRef::Array(array) }
    }

    const fn keyspace_id() -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }

    pub fn into_owned(self) -> TypeVertex<'static> {
        TypeVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TypeID {
    bytes: [u8; TypeID::LENGTH],
}

pub type TypeIDUInt = u16;

impl TypeID {
    pub(crate) const LENGTH: usize = std::mem::size_of::<TypeIDUInt>();

    pub fn new(bytes: [u8; TypeID::LENGTH]) -> TypeID {
        TypeID { bytes }
    }

    pub fn build(id: TypeIDUInt) -> Self {
        debug_assert_eq!(std::mem::size_of_val(&id), TypeID::LENGTH);
        TypeID { bytes: id.to_be_bytes() }
    }

    pub fn as_u16(&self) -> u16 {
        u16::from_be_bytes(self.bytes)
    }

    pub fn bytes(&self) -> [u8; TypeID::LENGTH] {
        self.bytes
    }
}
