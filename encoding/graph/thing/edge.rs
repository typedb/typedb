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
use crate::graph::thing::vertex_attribute::AttributeVertex;
use crate::graph::thing::vertex_object::ObjectVertex;
use crate::graph::type_::vertex::TypeVertex;
use crate::layout::infix::{InfixID, InfixType};

struct HasForwardEdge<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

/*
Extensions:

We could save 1 storage byte per edge by removing the prefix for from/to with a static prefix. However:
1) this adds significant complexity in the API (the from/to will not return a ref to a valid whole Vertex)
2) Making the API return valid vertices from the from/to would require an extra memcopy to add the known prefix
3) Alternatively, the API exposes incomplete vertices (just the Type+ID) and we deal with using them elsewhere
 */
impl<'a> HasForwardEdge<'a> {
    const LENGTH_PREFIX_FROM_OBJECT: usize = ObjectVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize = ObjectVertex::LENGTH + InfixID::LENGTH + AttributeVertex::LENGTH_PREFIX_TYPE;

    fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        HasForwardEdge { bytes: bytes }
    }

    fn build(from: &ObjectVertex<'_>, to: &AttributeVertex<'_>) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT + to.length());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        bytes.bytes_mut()[Self::range_to_for_vertex(to)].copy_from_slice(to.bytes().bytes());
        HasForwardEdge { bytes: ByteArrayOrRef::Array(bytes) }
    }

    pub fn prefix_from_object(from: &ObjectVertex<'_>) -> StorageKey<'static, { HasForwardEdge::LENGTH_PREFIX_FROM_OBJECT }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        StorageKey::new_owned(Self::keyspace_id(), bytes)
    }

    pub fn prefix_from_object_to_type(from: &ObjectVertex, to_type: &TypeVertex) -> StorageKey<'static, { HasForwardEdge::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        let to_type_range = Self::range_infix().end..Self::range_infix().end + TypeVertex::LENGTH;
        bytes.bytes_mut()[to_type_range].copy_from_slice(to_type.bytes().bytes());
        StorageKey::new_owned(Self::keyspace_id(), bytes)
    }

    fn keyspace_id() -> KeyspaceId {
        EncodingKeyspace::Data.id()
    }

    fn from(&'a self) -> ObjectVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        ObjectVertex::new(ByteArrayOrRef::Reference(reference))
    }

    fn to(&'a self) -> AttributeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[self.range_to()]);
        AttributeVertex::new(ByteArrayOrRef::Reference(reference))
    }

    const fn range_from() -> Range<usize> {
        0..ObjectVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_from().end..Self::range_from().end + InfixID::LENGTH
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    fn range_to(&self) -> Range<usize> {
        Self::range_infix().end..self.length()
    }

    fn range_to_for_vertex(to: &AttributeVertex) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + to.length()
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for HasForwardEdge<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for HasForwardEdge<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}
