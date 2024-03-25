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

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::{
        thing::{vertex_attribute::AttributeVertex, vertex_object::ObjectVertex},
        type_::vertex::TypeVertex,
    },
    layout::infix::{InfixID, InfixType},
    AsBytes, EncodingKeyspace, Keyable,
};

/*

[keyspace]
ThingEdgeHasAttribute8
ThingEdgeHasReverseAttribute8

[keyspace]
ThingEdgeHasAttribute16
ThingEdgeHasReverseAttribute16

[keyspace]
[object][role_type_id][relation] --> repetitions
+reverse



Edge layouts:
-- small attribute ownership, size = 11 + 1 + 11 = 23. Prefix = 11 + 1 = 12 and 11 + 1 + 3 = 15
[Object][has][Attribute_8]
[Object][has][Attribute_16]
[Attribute_8][has_reverse][Object]

-- large attribute ownership, size = 17 + 1 + 11 = 29. Prefix = 17 + 1 = 18 and 17 + 1 + 3 = 21
[Attribute_16][has_reverse][Object]

-- relation role player, size = 11 + 1 + 11 + 2 = 25. Prefix = 11 + 1 = 12 and 11 + 1 + 3 = 15
[Object][roleplayer][Relation][RoleTypeID]
[Relation][roleplayer_reverse][Object][RoleTypeID]

-- role player indexed, size = 11 + 1 + 11 + 11 + 2 + 2 = 38. Prefix = 11 + 1 = 12 and 11 + 1 + 3 = 15
[Object][roleplayer_index][Object][Relation][RoleType1][RoleType2] * note: no reverse type, undirected
Note: we may reorder this to include relation type in the infix? If so, have to reorder.

Technically could group by prefix extractors:
1. ThingEdgeSmallPrefix (12, 15)
2. ThingEdgeLargePrefix (18, 21)

We still need to decide if we will group all HAS together (probably sensible?),
and separately, all ROLEPLAYER together,
and separately, all ROLEPLAYER_INDEX together.

We could also group everything together.

Note: if we use Infixes to designate edge type, then we lose the ability to quickly search/drop indexes or ranges.
For example layout:
[EdgeHas][Object][Attribute]
[EdgeRP][Object][Relation]

[RPIndex][Object][Object][Relation]

With the RelIndex prefix, we only need to scan through [RelIndex][3 bytes indicating types included in the index] for each type that is affected.
Without the RelIndex prefix, we have to scan through [3 bytes indicating types included in the index] for each

*/


/*
[Object][roleplayer_index][Object][Relation][RoleID1][RoleID2]

This allows
1. optimal lookup from an object to all indexed neighbors (common case: small result set)
2. optimal lookup from an object to all indexed neighbors of a specific type (common case: tiny result set)
--> This is going to be very efficient in most cases, where we do not have supernodes.
--> Most supernodes are probably going to be supernodes relation and role structure (schema << supernode size), so putting relation types first is not useful.
--> Best solution for supernodes is to apply intersections to traverse away from them or into them.
 */
struct ThingEdgeRelationIndex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeRelationIndex<'a> {
    const RANGE_FROM: Range<usize> = 0..ObjectVertex::LENGTH;
    const RANGE_INFIX: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + InfixID::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_INFIX.end..Self::RANGE_INFIX.end + ObjectVertex::LENGTH;

    fn from(&'a self) -> ObjectVertex<'a> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_FROM])))
    }

    fn to(&'a self) -> ObjectVertex<'a> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_TO])))
    }
}

/*
TODO: choose role player edge direction
[Object][roleplayer_reverse][Relation][RoleTypeID]
[Relation][roleplayer][Object][RoleTypeID]

This allows
1. optimal lookup from an object to all relations (common case: small result set)
2. optimal lookup from an object to a specific relation type (common case: tiny result set)
--> This is very efficient in most cases, unless we have supernode/huge relations. In this case, role players might help partition the space
--> However, the best remedy is still intersections on the relations
 */
struct ThingEdgeRolePlayer<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

// TODO: implement separately from above so we can type the from/to correctly
struct ThingEdgeRolePlayerReverse<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

/*
[Attribute8][has_reverse][object]
OR
[Attribute16][has_reverse][object]

Note that these are represented here together, but belong in different keyspaces due to different prefix lengths
OR we can merge them and use multiple extractors?
 */
struct ThingEdgeHasReverse<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

/*
[object][has][attribute8]
or
[object][has][attribute16]
 */
struct ThingEdgeHas<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeHas<'a> {
    const LENGTH_PREFIX_FROM_OBJECT: usize = ObjectVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize =
        ObjectVertex::LENGTH + InfixID::LENGTH + AttributeVertex::LENGTH_PREFIX_TYPE;

    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        ThingEdgeHas { bytes }
    }

    fn build(from: &ObjectVertex<'_>, to: &AttributeVertex<'_>) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT + to.length());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        bytes.bytes_mut()[Self::range_to_for_vertex(to)].copy_from_slice(to.bytes().bytes());
        ThingEdgeHas { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from_object(
        from: &ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        StorageKey::new_owned(Self::KEYSPACE_ID, bytes)
    }

    pub fn prefix_from_object_to_type(
        from: &ObjectVertex,
        to_type: &TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_infix()].copy_from_slice(&InfixType::EdgeHas.infix_id().bytes());
        let to_type_range = Self::range_infix().end..Self::range_infix().end + TypeVertex::LENGTH;
        bytes.bytes_mut()[to_type_range].copy_from_slice(to_type.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE_ID, bytes)
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

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeHas<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeHas<'a> {
    const KEYSPACE_ID: EncodingKeyspace = EncodingKeyspace::Data;
}
