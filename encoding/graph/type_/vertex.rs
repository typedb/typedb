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


use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable, Prefixed};
use crate::graph::Typed;
use crate::layout::prefix::{PrefixID, PrefixType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_vertex_constructors {
    ($new_name:ident, $build_name:ident, PrefixType::$prefix:ident) => {
        pub fn $new_name<'a>(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> TypeVertex<'a> {
            let vertex = TypeVertex::new(bytes);
            debug_assert_eq!(vertex.prefix(), PrefixType::$prefix);
            vertex
        }

        pub fn $build_name(type_id: &TypeID) -> TypeVertex<'static> {
            TypeVertex::build(&PrefixType::$prefix.prefix_id(), type_id)
        }
    };
}

type_vertex_constructors!(new_entity_type_vertex, build_entity_type_vertex, PrefixType::VertexEntityType);
type_vertex_constructors!(new_attribute_type_vertex, build_attribute_type_vertex, PrefixType::VertexAttributeType);

impl<'a> TypeVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> TypeVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeVertex { bytes: bytes }
    }

    fn build(prefix: &PrefixID<'a>, type_id: &TypeID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_id.bytes().bytes());
        TypeVertex { bytes: ByteArrayOrRef::Array(array) }
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
        EncodingKeyspace::Schema.id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> { }

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {}

#[derive(Debug, PartialEq, Eq)]
pub struct TypeID<'a> {
    bytes: ByteArrayOrRef<'a, { TypeID::LENGTH }>,
}

pub type TypeIDUInt = u16;

impl<'a> TypeID<'a> {
    pub(crate) const LENGTH: usize = std::mem::size_of::<TypeIDUInt>();

    pub fn new(bytes: ByteArrayOrRef<'a, { TypeID::LENGTH }>) -> TypeID<'a> {
        debug_assert_eq!(bytes.length(), TypeID::LENGTH);
        TypeID { bytes: bytes }
    }

    pub fn build(id: TypeIDUInt) -> Self {
        debug_assert_eq!(std::mem::size_of_val(&id), TypeID::LENGTH);
        TypeID { bytes: ByteArrayOrRef::Array(ByteArray::inline(id.to_be_bytes(), TypeID::LENGTH)) }
    }

    pub(crate) fn as_u16(&self) -> u16 {
        u16::from_be_bytes(self.bytes.bytes()[0..Self::LENGTH].try_into().unwrap())
    }
}

impl<'a> AsBytes<'a, { TypeID::LENGTH }> for TypeID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { TypeID::LENGTH }> {
        self.bytes
    }
}

