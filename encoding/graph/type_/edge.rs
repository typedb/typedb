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
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable};
use crate::graph::type_::vertex::TypeVertex;
use crate::layout::infix::{InfixID, InfixType};

pub struct TypeEdge<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

macro_rules! type_edge_constructors {
    ($new_name:ident, $build_name:ident, InfixType::$infix:ident) => {
        pub(crate) fn $new_name<'a>(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> TypeEdge<'a> {
            let edge = TypeEdge::new(bytes);
            debug_assert_eq!(edge.infix(), InfixType::$infix);
            edge
        }

        pub(crate) fn $build_name(from: &TypeVertex, to: &TypeVertex) -> TypeEdge<'static> {
            TypeEdge::build(from, InfixType::$infix, to)
        }
    };
}

type_edge_constructors!(new_sub_edge_forward, build_sub_edge_forward, InfixType::SubForward);
type_edge_constructors!(new_sub_edge_backward, build_sub_edge_backward, InfixType::SubBackward);
type_edge_constructors!(new_owns_edge_forward, build_owns_edge_forward, InfixType::OwnsForward);
type_edge_constructors!(new_owns_edge_backward, build_owns_edge_backward, InfixType::OwnsBackward);
type_edge_constructors!(new_plays_edge_forward, build_plays_edge_forward, InfixType::PlaysForward);
type_edge_constructors!(new_plays_edge_backward, build_plays_edge_backward, InfixType::PlaysBackward);
type_edge_constructors!(new_relates_edge_forward, build_relates_edge_forward, InfixType::RelatesForward);
type_edge_constructors!(new_relates_edge_backward, build_relates_edge_backward,  InfixType::RelatesBackward);

impl<'a> TypeEdge<'a> {
    const LENGTH: usize = 2 * TypeVertex::LENGTH + InfixID::LENGTH;

    fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let edge = TypeEdge { bytes: bytes };
        edge
    }

    fn build(from: &TypeVertex, infix: InfixType, to: &TypeVertex) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(infix.infix_id().bytes().bytes());
        bytes.bytes_mut()[Self::range_to()].copy_from_slice(to.bytes().bytes());
        Self { bytes: ByteArrayOrRef::Array(bytes) }
    }

    fn from(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    fn to(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_to()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    fn infix(&self) -> InfixType {
        InfixType::from_infix_id(InfixID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_infix()]))))
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
        EncodingKeyspace::Schema.id()
    }
}
