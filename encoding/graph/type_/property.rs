/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::key_value::StorageKey;

use crate::{
    graph::type_::{
        edge::{TypeEdge, TypeEdgeEncoding},
        vertex::{TypeVertex, TypeVertexEncoding},
    },
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TypeVertexProperty<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> TypeVertexProperty<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    const PREFIX: Prefix = Prefix::PropertyTypeVertex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = TypeVertexProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    pub fn build(vertex: TypeVertex, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array[Self::range_type_vertex()].copy_from_slice(&vertex.into_bytes());
        array[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        TypeVertexProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        vertex: TypeVertex,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array[Self::range_type_vertex()].copy_from_slice(&vertex.into_bytes());
        array[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array[Self::range_suffix(suffix.length())].copy_from_slice(&suffix);
        TypeVertexProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeVertexProperty::PREFIX.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn type_vertex(&'a self) -> TypeVertex {
        TypeVertex::new(Bytes::reference(&self.bytes[Self::range_type_vertex()]))
    }

    pub fn infix(&self) -> Infix {
        let infix_bytes = &self.bytes[Self::range_infix()];
        Infix::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    fn suffix_length(&self) -> usize {
        self.bytes.len() - Self::LENGTH_NO_SUFFIX
    }

    pub fn suffix(&self) -> Option<&[u8]> {
        let suffix_length = self.suffix_length();
        if suffix_length > 0 {
            Some(&self.bytes[Self::range_suffix(self.suffix_length())])
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

pub trait TypeVertexPropertyEncoding<'a> {
    const INFIX: Infix;

    fn from_value_bytes(value: &[u8]) -> Self;

    fn build_key<'b>(vertex: impl TypeVertexEncoding<'b>) -> TypeVertexProperty<'b> {
        TypeVertexProperty::build(vertex.into_vertex(), Self::INFIX)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>>; // TODO: Can this be just Bytes?

    fn is_decodable_from(key_bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        key_bytes.length() == TypeVertexProperty::LENGTH_NO_SUFFIX
            && TypeVertexProperty::new(key_bytes).infix() == Self::INFIX
    }
}

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

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = TypeEdgeProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    pub fn build(edge: TypeEdge<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyTypeEdge.prefix_id().bytes());
        array[Self::range_type_edge()].copy_from_slice(&edge.into_bytes());
        array[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        TypeEdgeProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        edge: TypeEdge<'_>,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array[Self::RANGE_PREFIX].copy_from_slice(&Prefix::PropertyTypeEdge.prefix_id().bytes());
        array[Self::range_type_edge()].copy_from_slice(&edge.into_bytes());
        array[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array[Self::range_suffix(suffix.length())].copy_from_slice(&suffix);
        TypeEdgeProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeEdgeProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeEdgeProperty::PREFIX.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn type_edge(&'a self) -> TypeEdge<'a> {
        TypeEdge::new(Bytes::reference(&self.bytes[Self::range_type_edge()]))
    }

    pub fn infix(&self) -> Infix {
        let infix_bytes = &self.bytes[Self::range_infix()];
        Infix::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    fn suffix_length(&self) -> usize {
        self.bytes.len() - Self::LENGTH_NO_SUFFIX
    }

    pub fn suffix(&self) -> Option<&[u8]> {
        let suffix_length = self.suffix_length();
        if suffix_length > 0 {
            Some(&self.bytes[Self::range_suffix(suffix_length)])
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

pub trait TypeEdgePropertyEncoding<'a>: Sized {
    const INFIX: Infix;

    fn from_value_bytes(value: &[u8]) -> Self;

    fn build_key<'b>(edge: impl TypeEdgeEncoding<'b>) -> TypeEdgeProperty<'b> {
        TypeEdgeProperty::build(edge.to_canonical_type_edge(), Self::INFIX)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>>;

    fn is_decodable_from(key_bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        key_bytes.length() == TypeEdgeProperty::LENGTH_NO_SUFFIX
            && TypeEdgeProperty::new(key_bytes).infix() == Self::INFIX
    }
}
