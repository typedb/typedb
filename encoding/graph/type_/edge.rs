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

struct OwnsForwardEdge<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> OwnsForwardEdge<'a> {
    const LENGTH: usize = 2 * TypeVertex::LENGTH + InfixID::LENGTH;

    fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        OwnsForwardEdge { bytes: bytes, }
    }

    fn build(from: &TypeVertex, to: &TypeVertex) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(InfixType::OwnsForward.as_infix().bytes().bytes());
        bytes.bytes_mut()[Self::range_to()].copy_from_slice(to.bytes().bytes());
        OwnsForwardEdge { bytes: ByteArrayOrRef::Array(bytes) }
    }

    fn from(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    fn to(&'a self) -> TypeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_to()]);
        TypeVertex::new(ByteArrayOrRef::Reference(reference))
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

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for OwnsForwardEdge<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a,BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for OwnsForwardEdge<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }
}