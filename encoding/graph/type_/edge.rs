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
    graph::type_::vertex::TypeVertex,
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};


#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TypeEdge<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}


// TODO: If we parametrise this with <FROM, TO>, we could implement it for Owns, Plays, Relates instead
pub trait TypeEdgeEncoder {
    const PREFIX: Prefix;
    fn new_edge<'a>(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> TypeEdge<'a> {
        let edge = TypeEdge::new(bytes);
        debug_assert_eq!(edge.prefix(), Self::PREFIX);
        edge
    }
    fn build_edge<'a>(from: TypeVertex<'a>, to: TypeVertex<'a>) -> TypeEdge<'a> {
        TypeEdge::build(Self::PREFIX, from, to)
    }

    fn build_edge_prefix_from<'a>(from: TypeVertex<'a>) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        TypeEdge::build_prefix_from(Self::PREFIX, from)
    }
    fn build_edge_prefix_prefix(
        from_prefix: Prefix,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        TypeEdge::build_prefix_prefix(Self::PREFIX, from_prefix)
    }
    fn is_edge
    (bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        bytes.length() == TypeEdge::LENGTH && TypeEdge::new(bytes).prefix() == Self::PREFIX
    }
}

macro_rules! type_edge_constructor_trait_impl {
    ($encoder_name:ident, $prefix:ident) => {
        pub struct $encoder_name { }
        impl TypeEdgeEncoder for $encoder_name {
            const PREFIX : Prefix = Prefix::$prefix;
        }
    };
}

type_edge_constructor_trait_impl!(EdgeSubEncoder, EdgeSub);
type_edge_constructor_trait_impl!(EdgeSubReverseEncoder, EdgeSubReverse);

type_edge_constructor_trait_impl!(EdgeOwnsEncoder,EdgeOwns);
type_edge_constructor_trait_impl!(EdgeOwnsReverseEncoder,EdgeOwnsReverse);

type_edge_constructor_trait_impl!(EdgePlaysEncoder, EdgePlays);
type_edge_constructor_trait_impl!(EdgePlaysReverseEncoder, EdgePlaysReverse);

type_edge_constructor_trait_impl!(EdgeRelatesEncoder, EdgeRelates);
type_edge_constructor_trait_impl!(EdgeRelatesReverseEncoder, EdgeRelatesReverse);

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
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_to()].copy_from_slice(to.bytes().bytes());
        Self { bytes: Bytes::Array(bytes) }
    }

    pub fn build_prefix(prefix: Prefix) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_prefix(
        prefix: Prefix,
        from_prefix: Prefix,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH_PREFIX]
            .copy_from_slice(&from_prefix.prefix_id().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn build_prefix_from(
        prefix: Prefix,
        from: TypeVertex<'_>,
    ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn from(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        TypeVertex::new(Bytes::Reference(reference))
    }

    pub fn to(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_to()]);
        TypeVertex::new(Bytes::Reference(reference))
    }

    pub fn prefix(&self) -> Prefix {
        Prefix::from_prefix_id(PrefixID::new(self.bytes.bytes()[Self::RANGE_PREFIX].try_into().unwrap()))
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
