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

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable};
use crate::graph::type_::vertex::TypeVertex;
use crate::layout::infix::{InfixID, InfixType};

pub struct TypeEdge<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_edge_constructors {
    ($new_name:ident, $build_name:ident, $build_prefix:ident, $is_name:ident, InfixType::$infix:ident) => {
        pub fn $new_name<'a>(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> TypeEdge<'a> {
            let edge = TypeEdge::new(bytes);
            debug_assert_eq!(edge.infix(), InfixType::$infix);
            edge
        }

        pub fn $build_name(from: TypeVertex<'static>, to: TypeVertex<'static>) -> TypeEdge<'static> {
            TypeEdge::build(from, InfixType::$infix, to)
        }

        pub fn $build_prefix(from: TypeVertex<'static>) -> StorageKey<'static, {TypeEdge::LENGTH_PREFIX}> {
            TypeEdge::build_prefix(from, InfixType::$infix)
        }

        pub fn $is_name<'a>(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> bool {
            return bytes.length() == TypeEdge::LENGTH && TypeEdge::new(bytes).infix() == InfixType::$infix
        }
    };
}

type_edge_constructors!(
    new_edge_sub, build_edge_sub, build_edge_sub_prefix,
    is_edge_sub,
    InfixType::EdgeSub
);
type_edge_constructors!(
    new_edge_sub_reverse, build_edge_sub_reverse, build_edge_sub_reverse_prefix,
    is_edge_sub_reverse,
    InfixType::EdgeSubReverse
);
type_edge_constructors!(
    new_edge_owns, build_edge_owns, build_edge_owns_prefix,
    is_edge_owns,
    InfixType::EdgeOwns
);
type_edge_constructors!(
    new_edge_owns_reverse, build_edge_owns_reverse, build_edge_owns_reverse_prefix,
    is_edge_owns_reverse,
    InfixType::EdgeOwnsReverse
);
type_edge_constructors!(
    new_edge_plays, build_edge_plays, build_edge_plays_prefix,
    is_edge_plays,
    InfixType::EdgePlays
);
type_edge_constructors!(
    new_edge_plays_reverse, build_edge_plays_reverse, build_edge_plays_reverse_prefix,
    is_edge_plays_reverse,
    InfixType::EdgePlaysReverse
);
type_edge_constructors!(
    new_edge_relates, build_edge_relates, build_edge_relates_prefix,
    is_edge_relates,
    InfixType::EdgeRelates
);
type_edge_constructors!(
    new_edge_relates_reverse, build_edge_relates_reverse, build_edge_relates_reverse_prefix,
    is_edge_relates_reverse,
    InfixType::EdgeRelatesReverse
);

impl<'a> TypeEdge<'a> {
    const LENGTH: usize = 2 * TypeVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = TypeVertex::LENGTH + InfixID::LENGTH;

    fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let edge = TypeEdge { bytes: bytes };
        edge
    }

    fn build(from: TypeVertex, infix: InfixType, to: TypeVertex) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        bytes.bytes_mut()[Self::range_to()].copy_from_slice(to.bytes().bytes());
        Self { bytes: ByteArrayOrRef::Array(bytes) }
    }

    fn build_prefix(from: TypeVertex<'_>, infix: InfixType) -> StorageKey<'static, { TypeEdge::LENGTH_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        StorageKey::new_owned(Self::keyspace_id(), bytes)
    }

    pub fn from(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    pub fn to(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_to()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    fn infix(&self) -> InfixType {
        InfixType::from_infix_id(InfixID::new(self.bytes.bytes()[Self::range_infix()].try_into().unwrap()))
    }

    const fn keyspace_id() -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }

    const fn range_from() -> Range<usize> {
        0..TypeVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_from().end..Self::range_from().end + InfixID::LENGTH
    }

    const fn range_to() -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + TypeVertex::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeEdge<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeEdge<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}
