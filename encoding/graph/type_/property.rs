/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::key_value::StorageKey;

use crate::{
    graph::type_::{edge::TypeEdge, vertex::TypeVertex},
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};
use crate::graph::type_::edge::EncodableParametrisedTypeEdge;

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
            Prefix::from_prefix_id(PrefixID::new([bytes.bytes()[0]])) == TypeVertexProperty::PREFIX
                && $new_name(bytes).infix() == Infix::$infix
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

type_vertex_property_constructors!(
    new_property_type_annotation_distinct,
    build_property_type_annotation_distinct,
    is_property_type_annotation_distinct,
    InfixType::PropertyAnnotationDistinct
);

type_vertex_property_constructors!(
    new_property_type_annotation_independent,
    build_property_type_annotation_independent,
    is_property_type_annotation_independent,
    InfixType::PropertyAnnotationIndependent
);

type_vertex_property_constructors!(
    new_property_type_annotation_cardinality,
    build_property_type_annotation_cardinality,
    is_property_type_annotation_cardinality,
    InfixType::PropertyAnnotationCardinality
);

type_vertex_property_constructors!(
    new_property_type_annotation_regex,
    build_property_type_annotation_regex,
    is_property_type_annotation_regex,
    InfixType::PropertyAnnotationRegex
);

type_vertex_property_constructors!(
    new_property_type_ordering,
    build_property_type_ordering,
    is_property_type_ordering,
    InfixType::PropertyOrdering
);

impl<'a> TypeVertexProperty<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    const PREFIX: Prefix = Prefix::PropertyTypeVertex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeVertex::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = TypeVertexProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    pub fn build(vertex: TypeVertex<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
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
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array.bytes_mut()[Self::range_suffix(suffix.length())].copy_from_slice(suffix.bytes());
        TypeVertexProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeVertexProperty::PREFIX.prefix_id().bytes();
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

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TypeEdgeProperty<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> TypeEdgeProperty<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    const PREFIX: Prefix = Prefix::PropertyTypeEdge;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeEdge::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_EDGE: usize = PrefixID::LENGTH + TypeEdge::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = TypeEdgeProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    pub fn build(edge: TypeEdge<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyTypeEdge.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_edge()].copy_from_slice(edge.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        TypeEdgeProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        edge: TypeEdge<'_>,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyTypeEdge.prefix_id().bytes());
        array.bytes_mut()[Self::range_type_edge()].copy_from_slice(edge.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array.bytes_mut()[Self::range_suffix(suffix.length())].copy_from_slice(suffix.bytes());
        TypeEdgeProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeEdgeProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeEdgeProperty::PREFIX.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, ByteReference::new(&PREFIX_BYTES))
    }

    pub fn type_edge(&'a self) -> TypeEdge<'a> {
        TypeEdge::new(Bytes::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_type_edge()])))
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

    const fn range_type_edge() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeEdge::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_type_edge().end..Self::range_type_edge().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeEdgeProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeEdgeProperty<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeEdgeProperty<'a> {}

pub trait EncodableTypeEdgeProperty<'a> : Sized {

    const INFIX: Infix;

    fn decode_value<'b>(value: ByteReference<'b>) -> Self;

    fn build_key<'b>(edge: impl EncodableParametrisedTypeEdge<'b>) -> TypeEdgeProperty<'b> {
        TypeEdgeProperty::build(edge.to_canonical_type_edge(), Self::INFIX)
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>>; // TODO: Can this be just Bytes?
    fn is_property(key_bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        key_bytes.length() == TypeEdgeProperty::LENGTH_NO_SUFFIX
            && TypeEdgeProperty::new(key_bytes).infix() == Self::INFIX
    }
}
