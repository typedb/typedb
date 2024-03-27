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

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::type_::vertex::TypeVertex,
    layout::{
        infix::{InfixID, Infix},
        prefix::{PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};
use crate::layout::prefix::Prefix;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TypeVertexProperty<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_vertex_property_constructors {
    ($new_name:ident, $build_name:ident, $is_name:ident, InfixType::$infix:ident) => {
        pub fn $new_name(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> TypeVertexProperty<'_> {
            let vertex = TypeVertexProperty::new(bytes);
            debug_assert_eq!(vertex.infix(), Infix::$infix);
            vertex
        }

        pub fn $build_name(type_vertex: TypeVertex<'_>) -> TypeVertexProperty<'static> {
            TypeVertexProperty::build(type_vertex, Infix::$infix)
        }

        pub fn $is_name(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
            bytes.length() == TypeVertexProperty::LENGTH_NO_SUFFIX
                && TypeVertexProperty::new(bytes).infix() == Infix::$infix
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
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeVertex::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = TypeVertexProperty { bytes };
        debug_assert_eq!(property.prefix(), Prefix::PropertyType);
        property
    }

    fn build(vertex: TypeVertex<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyType.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        TypeVertexProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        vertex: TypeVertex<'_>,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyType.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array.bytes_mut()[Self::range_suffix(suffix.length())].copy_from_slice(suffix.bytes());
        TypeVertexProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = Prefix::PropertyType.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, ByteReference::new(&PREFIX_BYTES))
    }

    pub fn type_vertex(&'a self) -> TypeVertex<'a> {
        TypeVertex::new(Bytes::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_type_vertex()])))
    }

    pub fn infix(&self) -> Infix {
        let infix_bytes = &self.bytes.bytes()[Self::range_infix()];
        Infix::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    fn suffix_length(&self) -> usize {
        self.bytes().length() - Self::LENGTH_NO_SUFFIX
    }

    pub fn suffix(&self) -> Option<ByteReference> {
        let suffix_length = self.suffix_length();
        if suffix_length > 0 {
            Some(ByteReference::new(&self.bytes.bytes()[Self::range_suffix(self.suffix_length())]))
        } else {
            None
        }
    }

    const fn range_type_vertex() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_type_vertex().end..Self::range_type_vertex().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertexProperty<'a> {}
