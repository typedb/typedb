/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    ops::Range,
};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
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
            vertex_object::{ObjectID, ObjectVertex},
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
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::OptimisedPrefix15;
    const PREFIX: Prefix = Prefix::EdgeHas;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM_OBJECT: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;
    pub const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize =
        PrefixID::LENGTH + ObjectVertex::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    const LENGTH_BOUND: usize = PrefixID::LENGTH + ObjectVertex::LENGTH + AttributeVertex::MAX_LENGTH;

    pub fn new(from: ObjectVertex, to: AttributeVertex) -> Self {
        Self { owner: from, attribute: to }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let owner = ObjectVertex::decode(&bytes[Self::range_from()]);
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
        AttributeVertex::write_prefix_type(&mut bytes[Self::range_from().end..], Prefix::VertexAttribute, to_type_id);
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_object_to_type_with_attribute_prefix(
        from: ObjectVertex,
        attribute_vertex_prefix: &[u8],
    ) -> StorageKey<'static, { Self::LENGTH_BOUND }> {
        debug_assert!(
            attribute_vertex_prefix[AttributeVertex::INDEX_PREFIX] == AttributeVertex::PREFIX.prefix_id().byte
        );
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE + attribute_vertex_prefix.len());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from()].copy_from_slice(&from.to_bytes());
        let end = Self::range_from().end + attribute_vertex_prefix.len();
        bytes[Self::range_from().end..end].copy_from_slice(attribute_vertex_prefix);
        bytes.truncate(end);
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

impl Display for ThingEdgeHas {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let byte_layout = format!(
            r"(Reverse)
            Prefix:             [0..{}] = {}
            Owner:              [{:?}] = {}
            Attribute:          [{:?}] = {}
            ",
            Self::INDEX_PREFIX,
            self.prefix().prefix_id().byte,
            Self::range_from(),
            HexBytesFormatter::borrowed(&self.from().to_bytes()),
            self.range_to(),
            HexBytesFormatter::borrowed(&self.to().to_bytes()),
        );
        write!(f, "Edge: {:?}, byte layout: {}", self, byte_layout)
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
        let owner = ObjectVertex::decode(&bytes[attribute_len + 1..len]);
        Self { owner, attribute }
    }

    pub fn prefix_from_prefix_short(
        from_prefix: Prefix,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_PREFIX }> {
        Self::prefix_from_prefix(from_prefix, Self::keyspace_for_is_short(true))
    }

    pub fn prefix_from_prefix_long(
        from_prefix: Prefix,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_PREFIX }> {
        Self::prefix_from_prefix(from_prefix, Self::keyspace_for_is_short(false))
    }

    fn prefix_from_prefix(
        from_prefix: Prefix,
        keyspace: EncodingKeyspace,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + PrefixID::LENGTH]
            .copy_from_slice(&from_prefix.prefix_id().to_bytes());
        StorageKey::new_owned(keyspace, bytes)
    }

    pub fn prefix_from_attribute_type(
        from_type_value_type_category: ValueTypeCategory,
        from_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        let from_prefix_end = Self::INDEX_PREFIX + 1 + PrefixID::LENGTH;
        bytes[Self::INDEX_PREFIX + 1..from_prefix_end].copy_from_slice(&Prefix::VertexAttribute.prefix_id().to_bytes());
        let from_type_id_end = from_prefix_end + TypeID::LENGTH;
        bytes[from_prefix_end..from_type_id_end].copy_from_slice(&from_type_id.to_bytes());
        StorageKey::new_owned(
            Self::keyspace_for_is_short(AttributeVertex::is_category_short_encoding(from_type_value_type_category)),
            bytes,
        )
    }

    pub fn prefix_from_attribute_vertex_prefix(
        attribute_vertex_value_type_category: ValueTypeCategory,
        attribute_vertex_prefix: &[u8],
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        debug_assert!(
            attribute_vertex_prefix[AttributeVertex::INDEX_PREFIX] == AttributeVertex::PREFIX.prefix_id().byte
        );
        let mut bytes = ByteArray::zeros(Self::INDEX_PREFIX + 1 + attribute_vertex_prefix.len());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + attribute_vertex_prefix.len()]
            .copy_from_slice(attribute_vertex_prefix);
        let is_short_attribute = AttributeVertex::is_category_short_encoding(attribute_vertex_value_type_category);
        StorageKey::new_owned(Self::keyspace_for_is_short(is_short_attribute), bytes)
    }

    // TODO cleanup
    pub fn prefix_from_attribute(
        from: AttributeVertex,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_BOUND_PREFIX_FROM);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_from_for_vertex(from)].copy_from_slice(&from.to_bytes());
        bytes.truncate(Self::range_from_for_vertex(from).end);
        StorageKey::new_owned(Self::keyspace_for_is_short(from.is_short_encoding()), bytes)
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
        let is_from_short_encoding = from.is_short_encoding();
        StorageKey::new_owned(Self::keyspace_for_is_short(is_from_short_encoding), bytes)
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

    pub fn keyspace_for_is_short(is_short_attribute: bool) -> EncodingKeyspace {
        if is_short_attribute {
            EncodingKeyspace::OptimisedPrefix16
        } else {
            EncodingKeyspace::OptimisedPrefix25
        }
    }

    pub fn keyspace(&self) -> EncodingKeyspace {
        Self::keyspace_for_is_short(self.from().is_short_encoding())
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
        self.keyspace()
    }
}

impl Display for ThingEdgeHasReverse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let byte_layout = format!(
            r"(Reverse)
            Prefix:             [0..{}] = {}
            Attribute:          [{:?}] = {}
            Owner:              [{:?}] = {}
            ",
            Self::INDEX_PREFIX,
            self.prefix().prefix_id().byte,
            self.range_from(),
            HexBytesFormatter::borrowed(&self.from().to_bytes()),
            self.range_to(),
            HexBytesFormatter::borrowed(&self.to().to_bytes()),
        );
        write!(f, "Edge: {:?}, byte layout: {}", self, byte_layout)
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
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::OptimisedPrefix15;
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

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        match Prefix::from_prefix_id(PrefixID::new(bytes[Self::INDEX_PREFIX])) {
            Self::PREFIX => {
                let relation = ObjectVertex::decode(&bytes[Self::RANGE_FROM]);
                let player = ObjectVertex::decode(&bytes[Self::RANGE_TO]);
                let role_id = TypeID::decode(bytes[Self::RANGE_ROLE_ID].try_into().unwrap());
                Self { relation, player, role_id, is_reverse: false }
            }
            Self::PREFIX_REVERSE => {
                let player = ObjectVertex::decode(&bytes[Self::RANGE_FROM]);
                let relation = ObjectVertex::decode(&bytes[Self::RANGE_TO]);
                let role_id = TypeID::decode(bytes[Self::RANGE_ROLE_ID].try_into().unwrap());
                Self { relation, player, role_id, is_reverse: true }
            }
            _ => panic!(),
        }
    }

    pub fn new(relation: ObjectVertex, player: ObjectVertex, role: TypeVertex) -> Self {
        Self { relation, player, role_id: role.type_id_(), is_reverse: false }
    }

    pub fn new_reverse(player: ObjectVertex, relation: ObjectVertex, role: TypeVertex) -> Self {
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

    pub fn is_reverse(self) -> bool {
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

impl Display for ThingEdgeLinks {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let byte_layout = if self.is_reverse {
            format!(
                r"(Reverse)
                Prefix:             [0..{}] = {}
                Player:             [{:?}] = {}
                Relation:           [{:?}] = {}
                RoleID:             [{:?}] = {}
                ",
                Self::INDEX_PREFIX,
                Self::PREFIX_REVERSE.prefix_id().byte,
                Self::RANGE_FROM,
                HexBytesFormatter::borrowed(&self.player.to_bytes()),
                Self::RANGE_TO,
                HexBytesFormatter::borrowed(&self.relation.to_bytes()),
                Self::RANGE_ROLE_ID,
                HexBytesFormatter::borrowed(&self.role_id.to_bytes()),
            )
        } else {
            format!(
                r"(Canonical)
                Prefix:             [0..{}] = {}
                Relation:           [{:?}] = {}
                Player:             [{:?}] = {}
                RoleID:             [{:?}] = {}
                ",
                Self::INDEX_PREFIX,
                Self::PREFIX_REVERSE.prefix_id().byte,
                Self::RANGE_FROM,
                HexBytesFormatter::borrowed(&self.relation.to_bytes()),
                Self::RANGE_TO,
                HexBytesFormatter::borrowed(&self.player.to_bytes()),
                Self::RANGE_ROLE_ID,
                HexBytesFormatter::borrowed(&self.role_id.to_bytes()),
            )
        };
        write!(f, "Edge: {:?}, byte layout: {}", self, byte_layout)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ThingEdgeIndexedRelation {
    player_from: ObjectVertex,
    player_to: ObjectVertex,
    relation_type_id: TypeID,
    relation_id: ObjectID,
    role_id_from: TypeID,
    role_id_to: TypeID,
}

impl ThingEdgeIndexedRelation {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::OptimisedPrefix17;
    const PREFIX: Prefix = Prefix::EdgeLinksIndex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const RANGE_RELATION_TYPE_ID: Range<usize> = Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + TypeID::LENGTH;
    const RANGE_START: Range<usize> =
        Self::RANGE_RELATION_TYPE_ID.end..Self::RANGE_RELATION_TYPE_ID.end + ObjectVertex::LENGTH;
    const RANGE_END: Range<usize> = Self::RANGE_START.end..Self::RANGE_START.end + ObjectVertex::LENGTH;
    const RANGE_RELATION_ID: Range<usize> = Self::RANGE_END.end..Self::RANGE_END.end + ObjectID::LENGTH;
    const RANGE_START_ROLE_TYPE_ID: Range<usize> =
        Self::RANGE_RELATION_ID.end..Self::RANGE_RELATION_ID.end + TypeID::LENGTH;
    const RANGE_END_ROLE_TYPE_ID: Range<usize> =
        Self::RANGE_START_ROLE_TYPE_ID.end..Self::RANGE_START_ROLE_TYPE_ID.end + TypeID::LENGTH;
    const LENGTH: usize = Self::RANGE_END_ROLE_TYPE_ID.end;
    const RANGE_START_TYPE: Range<usize> =
        Self::RANGE_RELATION_TYPE_ID.end..Self::RANGE_RELATION_TYPE_ID.end + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_REL_TYPE_ID: usize = Self::RANGE_RELATION_TYPE_ID.end;
    pub const LENGTH_PREFIX_REL_TYPE_ID_START_TYPE: usize =
        PrefixID::LENGTH + TypeID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_REL_TYPE_ID_START: usize = Self::RANGE_START.end;
    pub const LENGTH_PREFIX_REL_TYPE_ID_START_END: usize = Self::RANGE_END.end;
    pub const LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION: usize = Self::RANGE_RELATION_ID.end;
    pub const LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION_START_ROLE: usize = Self::RANGE_START_ROLE_TYPE_ID.end;

    pub fn new(
        player_from: ObjectVertex,
        player_to: ObjectVertex,
        relation: ObjectVertex,
        role_id_from: TypeID,
        role_id_to: TypeID,
    ) -> Self {
        Self::new_from_relation_parts(
            player_from,
            player_to,
            relation.type_id_(),
            relation.object_id(),
            role_id_from,
            role_id_to,
        )
    }

    pub fn new_from_relation_parts(
        player_from: ObjectVertex,
        player_to: ObjectVertex,
        relation_type_id: TypeID,
        relation_id: ObjectID,
        role_id_from: TypeID,
        role_id_to: TypeID,
    ) -> Self {
        Self { player_from, player_to, relation_type_id, relation_id, role_id_from, role_id_to }
    }

    /// Byte layout: [rp_index][rel type][from_object][to_object][relation id][from_role_id][to_role_id]
    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let player_from = ObjectVertex::decode(&bytes[Self::RANGE_START]);
        let player_to = ObjectVertex::decode(&bytes[Self::RANGE_END]);
        let role_id_from = TypeID::decode(bytes[Self::RANGE_START_ROLE_TYPE_ID].try_into().unwrap());
        let role_id_to = TypeID::decode(bytes[Self::RANGE_END_ROLE_TYPE_ID].try_into().unwrap());

        let relation_type_id = TypeID::decode((&bytes[Self::RANGE_RELATION_TYPE_ID]).try_into().unwrap());
        let relation_id = ObjectID::decode((&bytes[Self::RANGE_RELATION_ID]).try_into().unwrap());
        Self::new_from_relation_parts(player_from, player_to, relation_type_id, relation_id, role_id_from, role_id_to)
    }

    pub fn prefix_relation_type(
        relation_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation_id.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_relation_type_start_type(
        relation_id: TypeID,
        start_instance_type: TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID_START_TYPE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation_id.to_bytes());
        let start_type_prefix = ObjectVertex::prefix_for_type(start_instance_type.prefix());
        let start_type_id = start_instance_type.type_id_();
        ObjectVertex::write_prefix_type(&mut bytes[Self::RANGE_START_TYPE], start_type_prefix, start_type_id);
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_start(
        relation_id: TypeID,
        start: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID_START);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation_id.to_bytes());
        bytes[Self::RANGE_START].copy_from_slice(&start.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_start_end(
        relation_id: TypeID,
        start: ObjectVertex,
        end: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START_END }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID_START_END);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation_id.to_bytes());
        bytes[Self::RANGE_START].copy_from_slice(&start.to_bytes());
        bytes[Self::RANGE_END].copy_from_slice(&end.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_start_end_relation(
        start: ObjectVertex,
        end: ObjectVertex,
        relation: ObjectVertex,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation.type_id_().to_bytes());
        bytes[Self::RANGE_START].copy_from_slice(&start.to_bytes());
        bytes[Self::RANGE_END].copy_from_slice(&end.to_bytes());
        bytes[Self::RANGE_RELATION_ID].copy_from_slice(&relation.object_id().to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_start_end_relation_startrole(
        start: ObjectVertex,
        end: ObjectVertex,
        relation: ObjectVertex,
        start_role: TypeVertex,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION_START_ROLE }>
    {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_REL_TYPE_ID_START_END_RELATION_START_ROLE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&relation.type_id_().to_bytes());
        bytes[Self::RANGE_START].copy_from_slice(&start.to_bytes());
        bytes[Self::RANGE_END].copy_from_slice(&end.to_bytes());
        bytes[Self::RANGE_RELATION_ID].copy_from_slice(&relation.object_id().to_bytes());
        bytes[Self::RANGE_START_ROLE_TYPE_ID].copy_from_slice(&start_role.type_id_().to_bytes());
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

    pub fn relation_type_id(&self) -> TypeID {
        self.relation_type_id
    }

    pub fn relation_id(&self) -> ObjectID {
        self.relation_id
    }

    pub fn relation(self) -> ObjectVertex {
        ObjectVertex::build_relation(self.relation_type_id, self.relation_id)
    }

    pub fn from_role_id(self) -> TypeID {
        self.role_id_from
    }

    pub fn to_role_id(self) -> TypeID {
        self.role_id_to
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ThingEdgeIndexedRelation {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_RELATION_TYPE_ID].copy_from_slice(&self.relation_type_id.to_bytes());
        bytes[Self::RANGE_START].copy_from_slice(&self.player_from.to_bytes());
        bytes[Self::RANGE_END].copy_from_slice(&self.player_to.to_bytes());
        bytes[Self::RANGE_RELATION_ID].copy_from_slice(&self.relation_id.to_bytes());
        bytes[Self::RANGE_START_ROLE_TYPE_ID].copy_from_slice(&self.role_id_from.to_bytes());
        bytes[Self::RANGE_END_ROLE_TYPE_ID].copy_from_slice(&self.role_id_to.to_bytes());
        Bytes::Array(bytes)
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ThingEdgeIndexedRelation {}

impl Keyable<BUFFER_KEY_INLINE> for ThingEdgeIndexedRelation {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Display for ThingEdgeIndexedRelation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let byte_layout: String = format!(
            r"
        Prefix:             [0..{}] = {}
        RelationType ID:    [{:?}] = {}
        From Player:        [{:?}] = {}
        To Player:          [{:?}] = {}
        Relation ID         [{:?}] = {}
        FromRoleID          [{:?}] = {}
        ToRoleID            [{:?}] = {}
        ",
            Self::INDEX_PREFIX,
            Self::PREFIX.prefix_id().byte,
            Self::RANGE_RELATION_TYPE_ID,
            HexBytesFormatter::borrowed(&self.relation_type_id.to_bytes()),
            Self::RANGE_START,
            HexBytesFormatter::borrowed(&self.player_from.to_bytes()),
            Self::RANGE_END,
            HexBytesFormatter::borrowed(&self.player_to.to_bytes()),
            Self::RANGE_RELATION_ID,
            HexBytesFormatter::borrowed(&self.relation_id.to_bytes()),
            Self::RANGE_START_ROLE_TYPE_ID,
            HexBytesFormatter::borrowed(&self.role_id_from.to_bytes()),
            Self::RANGE_END_ROLE_TYPE_ID,
            HexBytesFormatter::borrowed(&self.role_id_to.to_bytes()),
        );
        write!(f, "Edge: {:?}, byte layout: {}", self, byte_layout)
    }
}
