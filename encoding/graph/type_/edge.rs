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

pub mod edge_constructors {
    use bytes::Bytes;
    use storage::key_value::StorageKey;
    use crate::graph::type_::edge::TypeEdge;
    use crate::graph::type_::vertex::TypeVertex;
    use crate::layout::prefix::Prefix;
    use resource::constants::snapshot::BUFFER_KEY_INLINE;

    pub trait TypeEdgeConstructor {
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
        ($prefix_name:ident) => {
            pub struct $prefix_name { }
            impl TypeEdgeConstructor for $prefix_name {
                const PREFIX : Prefix = Prefix::$prefix_name;
            }
        };
    }

    type_edge_constructor_trait_impl!(EdgeSub);
    type_edge_constructor_trait_impl!(EdgeSubReverse);

    type_edge_constructor_trait_impl!(EdgeOwns);
    type_edge_constructor_trait_impl!(EdgeOwnsReverse);

    type_edge_constructor_trait_impl!(EdgePlays);
    type_edge_constructor_trait_impl!(EdgePlaysReverse);

    type_edge_constructor_trait_impl!(EdgeRelates);
    type_edge_constructor_trait_impl!(EdgeRelatesReverse);
}

macro_rules! type_edge_constructors {
    (Prefix::$prefix:ident, $new_name:ident, $build_name:ident, $build_prefix_from:ident, $build_prefix_prefix:ident, $is_name:ident) => {
        pub fn $new_name(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> TypeEdge<'_> {
            let edge = TypeEdge::new(bytes);
            debug_assert_eq!(edge.prefix(), Prefix::$prefix);
            edge
        }

        pub fn $build_name(from: TypeVertex<'static>, to: TypeVertex<'static>) -> TypeEdge<'static> {
            TypeEdge::build(Prefix::$prefix, from, to)
        }

        pub fn $build_prefix_from(from: TypeVertex<'static>) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM }> {
            TypeEdge::build_prefix_from(Prefix::$prefix, from)
        }

        pub fn $build_prefix_prefix(
            from_prefix: Prefix,
        ) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX_FROM_PREFIX }> {
            TypeEdge::build_prefix_prefix(Prefix::$prefix, from_prefix)
        }

        pub fn $is_name(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
            bytes.length() == TypeEdge::LENGTH && TypeEdge::new(bytes).prefix() == Prefix::$prefix
        }
    };
}

type_edge_constructors!(
    Prefix::EdgeSub,
    new_edge_sub,
    build_edge_sub,
    build_edge_sub_prefix_from,
    build_edge_sub_prefix_prefix,
    is_edge_sub
);
type_edge_constructors!(
    Prefix::EdgeSubReverse,
    new_edge_sub_reverse,
    build_edge_sub_reverse,
    build_edge_sub_reverse_prefix_from,
    build_edge_sub_reverse_prefix_prefix,
    is_edge_sub_reverse
);
type_edge_constructors!(
    Prefix::EdgeOwns,
    new_edge_owns,
    build_edge_owns,
    build_edge_owns_prefix_from,
    build_edge_owns_prefix_prefix,
    is_edge_owns
);
type_edge_constructors!(
    Prefix::EdgeOwnsReverse,
    new_edge_owns_reverse,
    build_edge_owns_reverse,
    build_edge_owns_reverse_prefix_from,
    build_edge_owns_reverse_prefix_prefix,
    is_edge_owns_reverse
);
type_edge_constructors!(
    Prefix::EdgePlays,
    new_edge_plays,
    build_edge_plays,
    build_edge_plays_prefix_from,
    build_edge_plays_prefix_prefix,
    is_edge_plays
);
type_edge_constructors!(
    Prefix::EdgePlaysReverse,
    new_edge_plays_reverse,
    build_edge_plays_reverse,
    build_edge_plays_reverse_prefix_from,
    build_edge_plays_reverse_prefix_prefix,
    is_edge_plays_reverse
);
type_edge_constructors!(
    Prefix::EdgeRelates,
    new_edge_relates,
    build_edge_relates,
    build_edge_relates_prefix_from,
    build_edge_relates_prefix_prefix,
    is_edge_relates
);
type_edge_constructors!(
    Prefix::EdgeRelatesReverse,
    new_edge_relates_reverse,
    build_edge_relates_reverse,
    build_edge_relates_reverse_prefix_from,
    build_edge_relates_reverse_prefix_prefix,
    is_edge_relates_reverse
);

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
