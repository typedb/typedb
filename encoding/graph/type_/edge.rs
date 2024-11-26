/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::type_::vertex::{TypeVertex, TypeVertexEncoding},
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TypeEdge<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> TypeEdge<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub(crate) const LENGTH: usize = PrefixID::LENGTH + 2 * TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX_FROM_PREFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH_PREFIX;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeEdge { bytes }
    }

    fn build(prefix: Prefix, from: TypeVertex, to: TypeVertex) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes[Self::range_to()].copy_from_slice(to.bytes().bytes());
        Self { bytes: Bytes::Array(bytes) }
    }

    pub fn build_prefix(prefix: Prefix) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX);
        bytes[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_prefix(
        prefix: Prefix,
        from_prefix: Prefix,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes[Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH_PREFIX]
            .copy_from_slice(&from_prefix.prefix_id().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_from(
        prefix: Prefix,
        from: TypeVertex<'_>,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes[Self::range_from()].copy_from_slice(from.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn from(&'a self) -> TypeVertex<'a> {
        TypeVertex::new(Bytes::reference(&self.bytes[Self::range_from()]))
    }

    pub fn to(&'a self) -> TypeVertex<'a> {
        TypeVertex::new(Bytes::reference(&self.bytes[Self::range_to()]))
    }

    pub fn prefix(&self) -> Prefix {
        Prefix::from_prefix_id(PrefixID::new(self.bytes[Self::RANGE_PREFIX].try_into().unwrap()))
    }

    const fn range_from() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH
    }

    const fn range_to() -> Range<usize> {
        Self::range_from().end..Self::range_from().end + TypeVertex::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeEdge<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeEdge<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeEdge<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

pub trait TypeEdgeEncoding<'a>: Sized {
    const CANONICAL_PREFIX: Prefix;
    const REVERSE_PREFIX: Prefix;

    type From: TypeVertexEncoding<'a>;
    type To: TypeVertexEncoding<'a>;

    fn from_vertices(from: Self::From, to: Self::To) -> Self;

    fn canonical_from(&self) -> Self::From;
    fn canonical_to(&self) -> Self::To;

    fn decode_canonical_edge(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        let type_edge = TypeEdge::new(bytes);
        debug_assert_eq!(type_edge.prefix(), Self::CANONICAL_PREFIX);
        Self::from_vertices(
            Self::From::from_vertex(type_edge.from().into_owned()).unwrap(),
            Self::To::from_vertex(type_edge.to().into_owned()).unwrap(),
        )
    }

    fn decode_reverse_edge(bytes: Bytes<'static, BUFFER_KEY_INLINE>) -> Self {
        let type_edge = TypeEdge::new(bytes);
        debug_assert_eq!(type_edge.prefix(), Self::REVERSE_PREFIX);
        Self::from_vertices(
            Self::From::from_vertex(type_edge.to().into_owned()).unwrap(),
            Self::To::from_vertex(type_edge.from().into_owned()).unwrap(),
        )
    }

    fn to_canonical_type_edge(&self) -> TypeEdge<'a> {
        TypeEdge::build(Self::CANONICAL_PREFIX, self.canonical_from().into_vertex(), self.canonical_to().into_vertex())
    }

    fn to_reverse_type_edge(&self) -> TypeEdge<'a> {
        TypeEdge::build(Self::REVERSE_PREFIX, self.canonical_to().into_vertex(), self.canonical_from().into_vertex())
    }

    fn prefix_for_canonical_edges_from(from: Self::From) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        TypeEdge::build_prefix_from(Self::CANONICAL_PREFIX, from.into_vertex())
    }

    fn prefix_for_reverse_edges_from(from: Self::To) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        TypeEdge::build_prefix_from(Self::REVERSE_PREFIX, from.into_vertex())
    }

    fn prefix_for_canonical_edges(from_prefix: Prefix) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        TypeEdge::build_prefix_prefix(Self::CANONICAL_PREFIX, from_prefix)
    }

    fn prefix_for_reverse_edges(from_prefix: Prefix) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        TypeEdge::build_prefix_prefix(Self::REVERSE_PREFIX, from_prefix)
    }

    fn is_canonical_edge(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        bytes.length() == TypeEdge::LENGTH && TypeEdge::new(bytes).prefix() == Self::CANONICAL_PREFIX
    }

    fn is_reverse_edge(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        bytes.length() == TypeEdge::LENGTH && TypeEdge::new(bytes).prefix() == Self::REVERSE_PREFIX
    }
}
