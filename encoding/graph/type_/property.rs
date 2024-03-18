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

use std::ops::Range;

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, keyspace::keyspace::KeyspaceId};

use crate::{
    graph::type_::vertex::TypeVertex,
    layout::{
        infix::{InfixID, InfixType},
        prefix::{PrefixID, PrefixType},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, PartialEq, Eq)]
pub struct TypeVertexProperty<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_vertex_property_constructors {
    ($new_name:ident, $build_name:ident, $is_name:ident, InfixType::$infix:ident) => {
        pub fn $new_name(bytes: ByteArrayOrRef<'_, BUFFER_KEY_INLINE>) -> TypeVertexProperty<'_> {
            let vertex = TypeVertexProperty::new(bytes);
            debug_assert_eq!(vertex.infix(), InfixType::$infix);
            vertex
        }

        pub fn $build_name(type_vertex: TypeVertex<'static>) -> TypeVertexProperty<'static> {
            TypeVertexProperty::build(type_vertex, InfixType::$infix)
        }

        pub fn $is_name(bytes: ByteArrayOrRef<'_, BUFFER_KEY_INLINE>) -> bool {
            bytes.length() == TypeVertexProperty::LENGTH && TypeVertexProperty::new(bytes).infix() == InfixType::$infix
        }
    };
}

type_vertex_property_constructors!(
    new_property_type_label,
    build_property_type_label,
    is_property_type_label_prefix,
    InfixType::PropertyLabel
);

type_vertex_property_constructors!(
    new_property_type_value_type,
    build_property_type_value_type,
    is_property_type_value_type,
    InfixType::PropertyValueType
);

type_vertex_property_constructors!(
    new_property_type_annotation_abstract,
    build_property_type_annotation_abstract,
    is_property_type_annotation_abstract,
    InfixType::PropertyAnnotationAbstract
);

impl<'a> TypeVertexProperty<'a> {
    const LENGTH: usize = PrefixID::LENGTH + TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeVertex::LENGTH;

    fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let property = TypeVertexProperty { bytes };
        debug_assert_eq!(property.prefix(), PrefixType::PropertyType);
        property
    }

    fn build(vertex: TypeVertex<'_>, infix: InfixType) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&PrefixType::PropertyType.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        TypeVertexProperty { bytes: ByteArrayOrRef::Array(array) }
    }

    // TODO: is it better to have a const fn that is a reference to owned memory, or
    //       to always induce a tiny copy have a non-const function?
    pub const fn build_prefix() -> StorageKey<'static, { TypeVertexProperty::LENGTH_PREFIX }> {
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = PrefixType::PropertyType.prefix_id().bytes();
        StorageKey::new_ref(Self::keyspace_id(), ByteReference::new(&PREFIX_BYTES))
    }

    pub fn type_vertex(&'a self) -> TypeVertex<'a> {
        TypeVertex::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_type_vertex()])))
    }

    fn infix(&self) -> InfixType {
        let infix_bytes = &self.bytes.bytes()[Self::range_infix()];
        InfixType::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    const fn keyspace_id() -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }

    const fn range_type_vertex() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_type_vertex().end..Self::range_type_vertex().end + InfixID::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {}
