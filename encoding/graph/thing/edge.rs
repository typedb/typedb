/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, ops::Range};

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::KeyspaceSet,
};

use crate::{
    graph::{
        thing::{
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_object::ObjectVertex,
            ThingVertex, THING_VERTEX_LENGTH_PREFIX_TYPE,
        },
        type_::vertex::{TypeID, TypeVertex},
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    value::value_type::ValueTypeCategory,
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

///
/// [has][object][Attribute8|Attribute17]
///
/// Note: mixed suffix lengths will in general be OK since we have a different attribute type prefix separating them
///
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ThingEdgeHas {
    owner: ObjectVertex,
    attribute: AttributeVertex,
}

impl ThingEdgeHas {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeHas;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM_OBJECT: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;
    pub const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize =
        PrefixID::LENGTH + ObjectVertex::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;

    pub fn new(from: ObjectVertex, to: AttributeVertex) -> Self {
        Self { owner: from, attribute: to }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let owner = ObjectVertex::new(&bytes[Self::range_from()]);
        let len = bytes.len();
        let attribute = AttributeVertex::decode(&bytes[Self::range_from().end..len]);
        Self { owner, attribute }
    }

    pub fn prefix_from_type(type_: TypeVertex) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        ObjectVertex::write_prefix_from_type_vertex(&mut bytes[Self::range_from_type()], type_);
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_type_parts(
        type_prefix: Prefix,
        type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        ObjectVertex::write_prefix_type(
            &mut bytes[Self::range_from_type()],
            ObjectVertex::prefix_for_type(type_prefix),
            type_id,
        );
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_object(from: ObjectVertex) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&from.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_object_to_type(
        from: ObjectVertex,
        to_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&from.to_bytes());
        let to_prefix = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, to_type_id);
        let to_type_range = Self::range_from().end..Self::range_from().end + to_prefix.length();
        bytes[to_type_range].copy_from_slice(to_prefix.bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX.prefix_id().to_bytes()))
    }

    pub fn is_has(key: &StorageKeyArray<BUFFER_KEY_INLINE>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && !key.bytes().is_empty()
            && key.bytes()[Self::INDEX_PREFIX] == Self::PREFIX.prefix_id().byte
    }

    pub fn from(self) -> ObjectVertex {
        self.owner
    }

    pub fn to(self) -> AttributeVertex {
        self.attribute
    }

    const fn range_from_type() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + THING_VERTEX_LENGTH_PREFIX_TYPE
    }

    const fn range_from() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + ObjectVertex::LENGTH
    }

    fn len(self) -> usize {
        Self::LENGTH_PREFIX_FROM_OBJECT + self.attribute.len()
    }

    fn range_to(self) -> Range<usize> {
        Self::range_from().end..self.len()
    }

    fn range_to_for_vertex(to: AttributeVertex) -> Range<usize> {
        Self::range_from().end..Self::range_from().end + to.len()
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ThingEdgeHas {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT + self.attribute.len());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&self.owner.to_bytes());
        bytes[Self::range_to_for_vertex(self.attribute)].copy_from_slice(&self.attribute.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ThingEdgeHas {}

impl Keyable<BUFFER_KEY_INLINE> for ThingEdgeHas {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

///
/// [has_reverse][attribute, 8 byte ID][object]
/// OR
/// [has_reverse][attribute, 16 byte ID][object]
///
/// Note that these are represented here together, but should go to different keyspaces due to different prefix lengths
///
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ThingEdgeHasReverse {
    attribute: AttributeVertex,
    owner: ObjectVertex,
}

impl ThingEdgeHasReverse {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;

    const PREFIX: Prefix = Prefix::EdgeHasReverse;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    pub const LENGTH_PREFIX_FROM_PREFIX: usize = PrefixID::LENGTH + PrefixID::LENGTH;
    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    const INDEX_FROM_VALUE_PREFIX: usize = Self::LENGTH_PREFIX_FROM_TYPE;
    pub const LENGTH_BOUND_PREFIX_FROM: usize =
        PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE + AttributeID::max_length();
    pub const LENGTH_BOUND_PREFIX_FROM_TO_TYPE: usize = PrefixID::LENGTH
        + THING_VERTEX_LENGTH_PREFIX_TYPE
        + AttributeID::max_length()
        + THING_VERTEX_LENGTH_PREFIX_TYPE;

    pub fn new(from: AttributeVertex, to: ObjectVertex) -> Self {
        Self { attribute: from, owner: to }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let attribute_len = AttributeVertex::RANGE_TYPE_ID.end
            + AttributeID::value_type_encoding_length(ValueTypeCategory::from_bytes([
                bytes[Self::INDEX_FROM_VALUE_PREFIX]
            ]));
        debug_assert_eq!(bytes.len() - attribute_len, 1 + ObjectVertex::LENGTH);
        let len = bytes.len();
        let attribute = AttributeVertex::decode(&bytes[1..attribute_len + 1]);
        let owner = ObjectVertex::new(&bytes[attribute_len + 1..len]);
        Self { owner, attribute }
    }

    pub fn prefix_from_prefix(
        from_prefix: Prefix,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + PrefixID::LENGTH]
            .copy_from_slice(&from_prefix.prefix_id().to_bytes());
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_type(
        from_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        let from_prefix_end = Self::INDEX_PREFIX + 1 + PrefixID::LENGTH;
        bytes[Self::INDEX_PREFIX + 1..from_prefix_end].copy_from_slice(&Prefix::VertexAttribute.prefix_id().to_bytes());
        let from_type_id_end = from_prefix_end + TypeID::LENGTH;
        bytes[from_prefix_end..from_type_id_end].copy_from_slice(&from_type_id.to_bytes());
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_vertex_prefix(
        attribute_vertex_prefix: &[u8],
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        debug_assert!(
            attribute_vertex_prefix[AttributeVertex::INDEX_PREFIX] == AttributeVertex::PREFIX.prefix_id().byte
        );
        let mut bytes = ByteArray::zeros(Self::INDEX_PREFIX + 1 + attribute_vertex_prefix.len());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + attribute_vertex_prefix.len()]
            .copy_from_slice(attribute_vertex_prefix);
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    // TODO cleanup
    pub fn prefix_from_attribute(
        from: AttributeVertex,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_BOUND_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from_for_vertex(from)].copy_from_slice(&from.to_bytes());
        bytes.truncate(Self::range_from_for_vertex(from).end);
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_to_type(
        from: AttributeVertex,
        to_type: TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }> {
        Self::prefix_from_attribute_to_type_parts(from, to_type.prefix(), to_type.type_id_())
    }

    pub fn prefix_from_attribute_to_type_parts(
        from: AttributeVertex,
        to_type_prefix: Prefix,
        to_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_BOUND_PREFIX_FROM_TO_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        let range_from = Self::range_from_for_vertex(from);
        bytes[range_from.clone()].copy_from_slice(&from.to_bytes());
        let to_type_range = range_from.end..range_from.end + TypeVertex::LENGTH;
        ObjectVertex::write_prefix_type(
            &mut bytes[to_type_range],
            ObjectVertex::prefix_for_type(to_type_prefix),
            to_type_id,
        );
        bytes.truncate(range_from.end + TypeVertex::LENGTH);
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_to_type_range(
        from: AttributeVertex,
        range_start: RangeStart<TypeVertex>,
        range_end: RangeEnd<TypeVertex>,
    ) -> KeyRange<StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }>> {
        KeyRange::new_fixed_width(
            range_start.map(|vertex| Self::prefix_from_attribute_to_type(from, *vertex)),
            range_end.map(|vertex| Self::prefix_from_attribute_to_type(from, *vertex)),
        )
    }
    // end TODO

    pub fn is_has_reverse(key: StorageKeyReference<'_>) -> bool {
        if !key.bytes().is_empty() && key.bytes()[Self::INDEX_PREFIX] == Self::PREFIX.prefix_id().byte {
            let edge = ThingEdgeHasReverse::decode(Bytes::Reference(key.bytes()));
            edge.keyspace().id() == key.keyspace_id()
        } else {
            false
        }
    }

    pub fn from(self) -> AttributeVertex {
        self.attribute
    }

    pub fn to(self) -> ObjectVertex {
        self.owner
    }

    fn range_from(self) -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + self.from_length()
    }

    #[allow(clippy::wrong_self_convention, reason = "`from` refers to the edge's source vertex")]
    fn from_length(self) -> usize {
        THING_VERTEX_LENGTH_PREFIX_TYPE + self.attribute.len()
    }

    fn range_to(self) -> Range<usize> {
        self.range_from().end..self.length()
    }

    fn length(self) -> usize {
        PrefixID::LENGTH + self.attribute.len() + self.owner.len()
    }

    fn range_from_for_vertex(from: AttributeVertex) -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + from.len()
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ThingEdgeHasReverse {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(PrefixID::LENGTH + self.attribute.len() + self.owner.len());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        let range_from = Self::range_from_for_vertex(self.attribute);
        bytes[range_from.clone()].copy_from_slice(&self.attribute.to_bytes());
        let range_to = range_from.end..range_from.end + self.owner.len();
        bytes[range_to].copy_from_slice(&self.owner.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ThingEdgeHasReverse {}

impl Keyable<BUFFER_KEY_INLINE> for ThingEdgeHasReverse {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

///
/// [rp][relation][object][role_id]
/// OR
/// [rp_reverse][object][relation][role_id]
///
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ThingEdgeLinks {
    relation: ObjectVertex,
    player: ObjectVertex,
    role_id: TypeID,
    is_reverse: bool,
}

impl PartialOrd for ThingEdgeLinks {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ThingEdgeLinks {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.is_reverse ^ other.is_reverse {
            // reverse comes after canonical, which is opposite ordering to booleans
            other.is_reverse.cmp(&self.is_reverse)
        } else if self.is_reverse {
            (self.player, self.relation, self.role_id).cmp(&(other.player, other.relation, other.role_id))
        } else {
            (self.relation, self.player, self.role_id).cmp(&(other.relation, other.player, other.role_id))
        }
    }
}

impl ThingEdgeLinks {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeLinks;
    const PREFIX_REVERSE: Prefix = Prefix::EdgeLinksReverse;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();
    pub const FIXED_WIDTH_ENCODING_REVERSE: bool = Self::PREFIX_REVERSE.fixed_width_keys();

    const RANGE_FROM: Range<usize> = Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + ObjectVertex::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + ObjectVertex::LENGTH;
    const RANGE_ROLE_ID: Range<usize> = Self::RANGE_TO.end..Self::RANGE_TO.end + TypeID::LENGTH;
    const LENGTH: usize = PrefixID::LENGTH + 2 * ObjectVertex::LENGTH + TypeID::LENGTH;
    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;
    pub const LENGTH_PREFIX_FROM_TO_TYPE: usize =
        PrefixID::LENGTH + ObjectVertex::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM_TO: usize = PrefixID::LENGTH + ObjectVertex::LENGTH + ObjectVertex::LENGTH;

    pub fn new(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        match Prefix::from_prefix_id(PrefixID::new(bytes[Self::INDEX_PREFIX])) {
            Self::PREFIX => {
                let relation = ObjectVertex::new(&bytes[Self::RANGE_FROM]);
                let player = ObjectVertex::new(&bytes[Self::RANGE_TO]);
                let role_id = TypeID::decode(bytes[Self::RANGE_ROLE_ID].try_into().unwrap());
                Self { relation, player, role_id, is_reverse: false }
            }
            Self::PREFIX_REVERSE => {
                let player = ObjectVertex::new(&bytes[Self::RANGE_FROM]);
                let relation = ObjectVertex::new(&bytes[Self::RANGE_TO]);
                let role_id = TypeID::decode(bytes[Self::RANGE_ROLE_ID].try_into().unwrap());
                Self { relation, player, role_id, is_reverse: true }
            }
            _ => panic!(),
        }
    }

    pub fn build_links(relation: ObjectVertex, player: ObjectVertex, role: TypeVertex) -> Self {
        Self { relation, player, role_id: role.type_id_(), is_reverse: false }
    }

    pub fn build_links_reverse(player: ObjectVertex, relation: ObjectVertex, role: TypeVertex) -> Self {
        Self { relation, player, role_id: role.type_id_(), is_reverse: true }
    }

    pub fn prefix_from_relation_type(
        relation_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        ObjectVertex::write_prefix_type(
            &mut bytes[Self::range_from_type()],
            ObjectVertex::prefix_for_type(Prefix::VertexRelationType),
            relation_type_id,
        );
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation(relation: ObjectVertex) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&relation.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation_player_type(
        relation: ObjectVertex,
        player_type: TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO_TYPE }> {
        Self::prefix_from_relation_player_type_parts(relation, player_type.prefix(), player_type.type_id_())
    }

    pub fn prefix_from_relation_player_type_parts(
        relation: ObjectVertex,
        player_type_prefix: Prefix,
        player_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(relation.to_bytes().as_ref());
        ObjectVertex::write_prefix_type(
            &mut bytes[Self::range_to_type()],
            ObjectVertex::prefix_for_type(player_type_prefix),
            player_type_id,
        );
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation_player(
        relation: ObjectVertex,
        player: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&relation.to_bytes());
        bytes[Self::RANGE_TO].copy_from_slice(&player.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player(
        player: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX_REVERSE.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&player.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_type(
        player_type_prefix: Prefix,
        player_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX_REVERSE.prefix_id().byte;
        ObjectVertex::write_prefix_type(
            &mut bytes[Self::range_from_type()],
            ObjectVertex::prefix_for_type(player_type_prefix),
            player_type_id,
        );
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_relation_type(
        player: ObjectVertex,
        relation_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX_REVERSE.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(player.to_bytes().as_ref());
        ObjectVertex::write_prefix_type(
            &mut bytes[Self::range_to_type()],
            ObjectVertex::prefix_for_type(Prefix::VertexRelationType),
            relation_type_id,
        );
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_relation(
        player: ObjectVertex,
        relation: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX_REVERSE.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&player.to_bytes());
        bytes[Self::RANGE_TO].copy_from_slice(&relation.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX.prefix_id().to_bytes()))
    }

    pub fn prefix_reverse() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX_REVERSE.prefix_id().to_bytes()))
    }

    const fn range_from_type() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + THING_VERTEX_LENGTH_PREFIX_TYPE
    }

    const fn range_to_type() -> Range<usize> {
        Self::LENGTH_PREFIX_FROM..Self::LENGTH_PREFIX_FROM + THING_VERTEX_LENGTH_PREFIX_TYPE
    }

    pub fn is_links(key: &StorageKeyArray<BUFFER_KEY_INLINE>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::INDEX_PREFIX] == Self::PREFIX.prefix_id().byte
    }

    pub fn is_links_reverse(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::INDEX_PREFIX] == Self::PREFIX_REVERSE.prefix_id().byte
    }

    pub fn from(self) -> ObjectVertex {
        if self.is_reverse {
            self.player
        } else {
            self.relation
        }
    }

    pub fn to(self) -> ObjectVertex {
        if self.is_reverse {
            self.relation
        } else {
            self.player
        }
    }

    pub fn relation(self) -> ObjectVertex {
        self.relation
    }

    pub fn player(self) -> ObjectVertex {
        self.player
    }

    fn is_reverse(self) -> bool {
        self.is_reverse
    }

    pub fn role_id(self) -> TypeID {
        self.role_id
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ThingEdgeLinks {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        if self.is_reverse() {
            bytes[Self::INDEX_PREFIX] = Self::PREFIX_REVERSE.prefix_id().byte;
            bytes[Self::RANGE_FROM].copy_from_slice(&self.player.to_bytes());
            bytes[Self::RANGE_TO].copy_from_slice(&self.relation.to_bytes());
        } else {
            bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
            bytes[Self::RANGE_FROM].copy_from_slice(&self.relation.to_bytes());
            bytes[Self::RANGE_TO].copy_from_slice(&self.player.to_bytes());
        }
        bytes[Self::RANGE_ROLE_ID].copy_from_slice(&self.role_id.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ThingEdgeLinks {}

impl Keyable<BUFFER_KEY_INLINE> for ThingEdgeLinks {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

///
/// [rp_index][from_object][to_object][relation][from_role_id][to_role_id]
///
#[derive(Copy, Clone, Debug)]
pub struct ThingEdgeRolePlayerIndex {
    player_from: ObjectVertex,
    player_to: ObjectVertex,
    relation: ObjectVertex,
    role_id_from: TypeID,
    role_id_to: TypeID,
}

impl ThingEdgeRolePlayerIndex {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeRolePlayerIndex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const RANGE_FROM: Range<usize> = Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + ObjectVertex::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + ObjectVertex::LENGTH;
    const RANGE_RELATION: Range<usize> = Self::RANGE_TO.end..Self::RANGE_TO.end + ObjectVertex::LENGTH;
    const RANGE_FROM_ROLE_TYPE_ID: Range<usize> = Self::RANGE_RELATION.end..Self::RANGE_RELATION.end + TypeID::LENGTH;
    const RANGE_TO_ROLE_TYPE_ID: Range<usize> =
        Self::RANGE_FROM_ROLE_TYPE_ID.end..Self::RANGE_FROM_ROLE_TYPE_ID.end + TypeID::LENGTH;
    const LENGTH: usize = PrefixID::LENGTH + 3 * ObjectVertex::LENGTH + 2 * TypeID::LENGTH;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;

    pub fn new(
        player_from: ObjectVertex,
        player_to: ObjectVertex,
        relation: ObjectVertex,
        role_id_from: TypeID,
        role_id_to: TypeID,
    ) -> Self {
        Self { player_from, player_to, relation, role_id_from, role_id_to }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let player_from = ObjectVertex::new(&bytes[Self::RANGE_FROM]);
        let player_to = ObjectVertex::new(&bytes[Self::RANGE_TO]);
        let relation = ObjectVertex::new(&bytes[Self::RANGE_RELATION]);
        let role_id_from = TypeID::decode(bytes[Self::RANGE_FROM_ROLE_TYPE_ID].try_into().unwrap());
        let role_id_to = TypeID::decode(bytes[Self::RANGE_TO_ROLE_TYPE_ID].try_into().unwrap());
        Self { player_from, player_to, relation, role_id_from, role_id_to }
    }

    pub fn prefix_from(from: ObjectVertex) -> StorageKey<'static, { ThingEdgeRolePlayerIndex::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&from.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn is_index(key: &StorageKeyArray<BUFFER_KEY_INLINE>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::INDEX_PREFIX] == Self::PREFIX.prefix_id().byte
    }

    pub fn from(self) -> ObjectVertex {
        self.player_from
    }

    pub fn to(self) -> ObjectVertex {
        self.player_to
    }

    pub fn relation(self) -> ObjectVertex {
        self.relation
    }

    pub fn from_role_id(self) -> TypeID {
        self.role_id_from
    }

    pub fn to_role_id(self) -> TypeID {
        self.role_id_to
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_FROM].copy_from_slice(&self.player_from.to_bytes());
        bytes[Self::RANGE_TO].copy_from_slice(&self.player_to.to_bytes());
        bytes[Self::RANGE_RELATION].copy_from_slice(&self.relation.to_bytes());
        bytes[Self::RANGE_FROM_ROLE_TYPE_ID].copy_from_slice(&self.role_id_from.to_bytes());
        bytes[Self::RANGE_TO_ROLE_TYPE_ID].copy_from_slice(&self.role_id_to.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex {}

impl Keyable<BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}
