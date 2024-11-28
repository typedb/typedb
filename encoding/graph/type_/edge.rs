/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::type_::vertex::{TypeVertex, TypeVertexEncoding},
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct TypeEdge {
    prefix: Prefix,
    from: TypeVertex,
    to: TypeVertex,
}

impl TypeEdge {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub(crate) const LENGTH: usize = PrefixID::LENGTH + 2 * TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX_FROM_PREFIX: usize = PrefixID::LENGTH + TypeVertex::LENGTH_PREFIX;

    pub fn new(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let prefix = Prefix::from_prefix_id(PrefixID::new(bytes[Self::INDEX_PREFIX]));
        let from = TypeVertex::new(bytes.clone().into_range(Self::range_from()));
        let to = TypeVertex::new(bytes.clone().into_range(Self::range_to()));
        Self { prefix, from, to }
    }

    fn build(prefix: Prefix, from: TypeVertex, to: TypeVertex) -> Self {
        Self { prefix, from, to }
    }

    pub fn build_prefix(prefix: Prefix) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX);
        bytes[Self::INDEX_PREFIX] = prefix.prefix_id().byte;
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_prefix(
        prefix: Prefix,
        from_prefix: Prefix,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes[Self::INDEX_PREFIX] = prefix.prefix_id().byte;
        bytes[Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + TypeVertex::LENGTH_PREFIX]
            .copy_from_slice(&from_prefix.prefix_id().to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_from(prefix: Prefix, from: TypeVertex) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = prefix.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&from.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn from(self) -> TypeVertex {
        self.from
    }

    pub fn to(self) -> TypeVertex {
        self.to
    }

    pub fn prefix(self) -> Prefix {
        self.prefix
    }

    const fn range_from() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + TypeVertex::LENGTH
    }

    const fn range_to() -> Range<usize> {
        Self::range_from().end..Self::range_from().end + TypeVertex::LENGTH
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for TypeEdge {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes[Self::INDEX_PREFIX] = self.prefix.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&self.from.to_bytes());
        bytes[Self::range_to()].copy_from_slice(&self.to.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for TypeEdge {}

impl Keyable<BUFFER_KEY_INLINE> for TypeEdge {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

pub trait TypeEdgeEncoding: Sized {
    const CANONICAL_PREFIX: Prefix;
    const REVERSE_PREFIX: Prefix;

    type From: TypeVertexEncoding;
    type To: TypeVertexEncoding;

    fn from_vertices(from: Self::From, to: Self::To) -> Self;

    fn canonical_from(&self) -> Self::From;
    fn canonical_to(&self) -> Self::To;

    fn decode_canonical_edge(bytes: Bytes<'static, BUFFER_KEY_INLINE>) -> Self {
        let type_edge = TypeEdge::new(bytes);
        debug_assert_eq!(type_edge.prefix(), Self::CANONICAL_PREFIX);
        Self::from_vertices(
            Self::From::from_vertex(type_edge.from()).unwrap(),
            Self::To::from_vertex(type_edge.to()).unwrap(),
        )
    }

    fn decode_reverse_edge(bytes: Bytes<'static, BUFFER_KEY_INLINE>) -> Self {
        let type_edge = TypeEdge::new(bytes);
        debug_assert_eq!(type_edge.prefix(), Self::REVERSE_PREFIX);
        Self::from_vertices(
            Self::From::from_vertex(type_edge.to()).unwrap(),
            Self::To::from_vertex(type_edge.from()).unwrap(),
        )
    }

    fn to_canonical_type_edge(&self) -> TypeEdge {
        TypeEdge::build(Self::CANONICAL_PREFIX, self.canonical_from().into_vertex(), self.canonical_to().into_vertex())
    }

    fn to_reverse_type_edge(&self) -> TypeEdge {
        TypeEdge::build(Self::REVERSE_PREFIX, self.canonical_to().into_vertex(), self.canonical_from().into_vertex())
    }

    fn prefix_for_canonical_edges_from(from: Self::From) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
        TypeEdge::build_prefix_from(Self::CANONICAL_PREFIX, from.into_vertex())
    }

    fn prefix_for_reverse_edges_from(from: Self::To) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
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
