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

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{AsBytes, EncodingKeyspace, graph::{
    thing::{vertex_attribute::AttributeVertex, vertex_object::ObjectVertex},
    type_::vertex::TypeVertex,
}, Keyable, layout::infix::{InfixID}, Prefixed};
use crate::graph::type_::vertex::TypeID;
use crate::layout::prefix::{PrefixID, Prefix};

///
/// [rp_index][from_object][to_object][relation_id][from_role_id][to_role_id]
///
struct ThingEdgeRelationIndex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
    repetitions: u64,
}

impl<'a> ThingEdgeRelationIndex<'a> {
    const RANGE_FROM: Range<usize> = Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + ObjectVertex::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + ObjectVertex::LENGTH;
    const RANGE_RELATION_ID: Range<usize> = Self::RANGE_TO.end..Self::RANGE_TO.end + TypeID::LENGTH;
    const RANGE_FROM_ROLE_ID: Range<usize> = Self::RANGE_RELATION_ID.end..Self::RANGE_RELATION_ID.end + TypeID::LENGTH;
    const RANGE_TO_ROLE_ID: Range<usize> = Self::RANGE_FROM_ROLE_ID.end..Self::RANGE_FROM_ROLE_ID.end + TypeID::LENGTH;

    fn prefix(&'a self) -> PrefixID {
        PrefixID::new(self.bytes.bytes()[Self::RANGE_PREFIX].try_into().unwrap())
    }

    fn from(&'a self) -> ObjectVertex<'a> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_FROM])))
    }

    fn to(&'a self) -> ObjectVertex<'a> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_TO])))
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeRelationIndex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ThingEdgeRelationIndex<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeRelationIndex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Data
    }
}


///
/// [rp][object][relation][role_id]
///
struct ThingEdgeRolePlayer<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

///
/// [rp_reverse][relation][object][role_id]
///
struct ThingEdgeRolePlayerReverse<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}


///
/// [has][object][Attribute8|Attribute16]
///
/// Note: mixed suffix lengths will in general be OK since we have a different attribute type prefix separating them
///
struct ThingEdgeHas<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeHas<'a> {
    const LENGTH_PREFIX_FROM_OBJECT: usize = ObjectVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize =
        ObjectVertex::LENGTH + InfixID::LENGTH + AttributeVertex::LENGTH_PREFIX_TYPE;

    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.bytes()[Self::RANGE_PREFIX], Prefix::EdgeHas.prefix_id().bytes());
        ThingEdgeHas { bytes }
    }

    fn build(from: &ObjectVertex<'_>, to: &AttributeVertex<'_>) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT + to.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::EdgeHas.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_to_for_vertex(to)].copy_from_slice(to.bytes().bytes());
        ThingEdgeHas { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from_object(
        from: &ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::EdgeHas.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        StorageKey::new_owned(Self::keyspace(), bytes)
    }

    pub fn prefix_from_object_to_type(
        from: &ObjectVertex,
        to_type: &TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::EdgeHas.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        let to_type_range = Self::range_from().end..Self::range_from().end + TypeVertex::LENGTH;
        bytes.bytes_mut()[to_type_range].copy_from_slice(to_type.bytes().bytes());
        StorageKey::new_owned(Self::keyspace(), bytes)
    }

    fn from(&'a self) -> ObjectVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        ObjectVertex::new(Bytes::Reference(reference))
    }

    fn to(&'a self) -> AttributeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[self.range_to()]);
        AttributeVertex::new(Bytes::Reference(reference))
    }

    const fn range_from() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + ObjectVertex::LENGTH
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    fn range_to(&self) -> Range<usize> {
        Self::range_from().end..self.length()
    }

    fn range_to_for_vertex(to: &AttributeVertex) -> Range<usize> {
        Self::range_from().end..Self::range_from().end + to.length()
    }

    const fn keyspace() -> EncodingKeyspace {
        EncodingKeyspace::Data
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeHas<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ThingEdgeHas<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeHas<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::keyspace()
    }
}

///
/// [has_reverse][Attribute8][object]
/// OR
/// [has_reverse][Attribute16][object]
///
/// Note that these are represented here together, but belong in different keyspaces due to different prefix lengths
///
struct ThingEdgeHasReverse<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}