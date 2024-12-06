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
pub struct TypeVertexProperty {
    type_: TypeVertex,
    infix: Infix,
    suffix: Option<ByteArray<0>>,
}

impl TypeVertexProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyTypeVertex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(type_: TypeVertex, infix: Infix) -> Self {
        Self { type_, infix, suffix: None }
    }

    fn new_suffixed<const INLINE_BYTES: usize>(
        vertex: TypeVertex,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        Self { type_: vertex, infix, suffix: Some(ByteArray::copy(&suffix)) }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let type_ = TypeVertex::decode(bytes.clone().into_range(Self::range_type_vertex()));
        let infix = Infix::from_infix_id(InfixID::new((&bytes[Self::range_infix()]).try_into().unwrap()));
        let suffix =
            (bytes.length() > Self::LENGTH_NO_SUFFIX).then(|| ByteArray::copy(&bytes[Self::LENGTH_NO_SUFFIX..]));
        Self { type_, infix, suffix }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeVertexProperty::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn type_vertex(&self) -> TypeVertex {
        self.type_
    }

    pub fn infix(&self) -> Infix {
        self.infix
    }

    fn suffix_length(&self) -> usize {
        self.suffix.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    pub fn suffix(&self) -> Option<&[u8]> {
        self.suffix.as_deref()
    }

    const fn range_type_vertex() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + TypeVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_type_vertex().end..Self::range_type_vertex().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for TypeVertexProperty {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + self.suffix_length());
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_type_vertex()].copy_from_slice(&self.type_.to_bytes());
        array[Self::range_infix()].copy_from_slice(&self.infix.infix_id().bytes());
        if let Some(suffix) = self.suffix() {
            array[Self::range_suffix(suffix.len())].copy_from_slice(suffix);
        }
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for TypeVertexProperty {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for TypeVertexProperty {}

pub trait TypeVertexPropertyEncoding {
    const INFIX: Infix;

    fn from_value_bytes(value: &[u8]) -> Self;

    fn build_key(vertex: impl TypeVertexEncoding) -> TypeVertexProperty {
        TypeVertexProperty::new(vertex.into_vertex(), Self::INFIX)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>>; // TODO: Can this be just Bytes?

    fn is_decodable_from(key_bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        key_bytes.length() == TypeVertexProperty::LENGTH_NO_SUFFIX
            && TypeVertexProperty::decode(key_bytes).infix() == Self::INFIX
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TypeEdgeProperty {
    edge: TypeEdge,
    infix: Infix,
    suffix: Option<ByteArray<0>>,
}

impl TypeEdgeProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyTypeEdge;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + TypeEdge::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(edge: TypeEdge, infix: Infix) -> Self {
        Self { edge, infix, suffix: None }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let edge = TypeEdge::decode(bytes.clone().into_range(Self::range_type_edge()));
        let infix = Infix::from_infix_id(InfixID::new((&bytes[Self::range_infix()]).try_into().unwrap()));
        let suffix =
            (bytes.length() > Self::LENGTH_NO_SUFFIX).then(|| ByteArray::copy(&bytes[Self::LENGTH_NO_SUFFIX..]));
        Self { edge, infix, suffix }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        edge: TypeEdge,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        Self { edge, infix, suffix: Some(ByteArray::copy(&suffix)) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeEdgeProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = TypeEdgeProperty::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn type_edge(&self) -> TypeEdge {
        self.edge
    }

    pub fn infix(&self) -> Infix {
        self.infix
    }

    fn suffix_length(&self) -> usize {
        self.suffix.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    pub fn suffix(&self) -> Option<&[u8]> {
        self.suffix.as_deref()
    }

    const fn range_type_edge() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + TypeEdge::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_type_edge().end..Self::range_type_edge().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for TypeEdgeProperty {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + self.suffix_length());
        array[Self::INDEX_PREFIX] = Prefix::PropertyTypeEdge.prefix_id().byte;
        array[Self::range_type_edge()].copy_from_slice(&self.edge.to_bytes());
        array[Self::range_infix()].copy_from_slice(&self.infix.infix_id().bytes());
        if let Some(suffix) = self.suffix() {
            array[Self::range_suffix(suffix.len())].copy_from_slice(suffix);
        }
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for TypeEdgeProperty {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for TypeEdgeProperty {}

pub trait TypeEdgePropertyEncoding: Sized {
    const INFIX: Infix;

    fn from_value_bytes(value: &[u8]) -> Self;

    fn build_key(edge: impl TypeEdgeEncoding) -> TypeEdgeProperty {
        TypeEdgeProperty::new(edge.to_canonical_type_edge(), Self::INFIX)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>>;

    fn is_decodable_from(key_bytes: Bytes<'static, BUFFER_KEY_INLINE>) -> bool {
        key_bytes.length() == TypeEdgeProperty::LENGTH_NO_SUFFIX
            && TypeEdgeProperty::decode(key_bytes).infix() == Self::INFIX
    }
}
