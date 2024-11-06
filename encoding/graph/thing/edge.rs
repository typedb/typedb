/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyReference},
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ThingEdgeHas<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeHas<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeHas;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM_OBJECT: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;
    pub const LENGTH_PREFIX_FROM_OBJECT_TO_TYPE: usize =
        PrefixID::LENGTH + ObjectVertex::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.bytes()[Self::RANGE_PREFIX], Self::PREFIX.prefix_id().bytes());
        ThingEdgeHas { bytes }
    }

    pub fn build<'b>(from: ObjectVertex<'b>, to: AttributeVertex<'b>) -> ThingEdgeHas<'static> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT + to.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::range_to_for_vertex(to.as_reference())].copy_from_slice(to.bytes().bytes());
        ThingEdgeHas { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from_type(
        type_: TypeVertex<'static>,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from_type()]
            .copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(type_).bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_object(
        from: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_object_to_type(
        from: ObjectVertex,
        to_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from()].copy_from_slice(from.bytes().bytes());
        let to_prefix = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, to_type_id);
        let to_type_range = Self::range_from().end..Self::range_from().end + to_prefix.length();
        bytes.bytes_mut()[to_type_range].copy_from_slice(to_prefix.bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX.prefix_id().bytes()))
    }

    pub fn is_has(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && !key.bytes().is_empty()
            && key.bytes()[Self::RANGE_PREFIX] == Self::PREFIX.prefix_id().bytes()
    }

    pub fn from(&'a self) -> ObjectVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[Self::range_from()]);
        ObjectVertex::new(Bytes::Reference(reference))
    }

    pub fn to(&'a self) -> AttributeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[self.range_to()]);
        AttributeVertex::new(Bytes::Reference(reference))
    }

    pub fn into_from(self) -> ObjectVertex<'a> {
        let range = Self::range_from();
        ObjectVertex::new(self.bytes.into_range(range))
    }

    pub fn into_to(self) -> AttributeVertex<'a> {
        let range = self.range_to();
        AttributeVertex::new(self.bytes.into_range(range))
    }

    const fn range_from_type() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + THING_VERTEX_LENGTH_PREFIX_TYPE
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

    fn range_to_for_vertex(to: AttributeVertex) -> Range<usize> {
        Self::range_from().end..Self::range_from().end + to.length()
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ThingEdgeHasReverse<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeHasReverse<'a> {
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

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> ThingEdgeHasReverse<'a> {
        debug_assert_eq!(bytes.bytes()[Self::RANGE_PREFIX], Self::PREFIX.prefix_id().bytes());
        ThingEdgeHasReverse { bytes }
    }

    pub fn build(from: AttributeVertex<'_>, to: ObjectVertex<'_>) -> Self {
        let mut bytes = ByteArray::zeros(PrefixID::LENGTH + from.length() + to.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        let range_from = Self::range_from_for_vertex(from.as_reference());
        bytes.bytes_mut()[range_from.clone()].copy_from_slice(from.bytes().bytes());
        let range_to = range_from.end..range_from.end + to.length();
        bytes.bytes_mut()[range_to].copy_from_slice(to.bytes().bytes());
        ThingEdgeHasReverse { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from_prefix(
        from_prefix: Prefix,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_PREFIX }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_PREFIX);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + PrefixID::LENGTH]
            .copy_from_slice(&from_prefix.prefix_id().bytes);
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_type(
        from_type_id: TypeID,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        let from_prefix_end = Self::RANGE_PREFIX.end + PrefixID::LENGTH;
        bytes.bytes_mut()[Self::RANGE_PREFIX.end..from_prefix_end]
            .copy_from_slice(&Prefix::VertexAttribute.prefix_id().bytes);
        let from_type_id_end = from_prefix_end + TypeID::LENGTH;
        bytes.bytes_mut()[from_prefix_end..from_type_id_end].copy_from_slice(&from_type_id.bytes());
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_vertex_prefix(
        attribute_vertex_prefix: ByteReference<'_>,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let prefix = Prefix::from_prefix_id(PrefixID::new(
            attribute_vertex_prefix.bytes()[AttributeVertex::RANGE_PREFIX].try_into().unwrap(),
        ));
        debug_assert!(
            Prefix::ATTRIBUTE_MIN.prefix_id().bytes() <= prefix.prefix_id().bytes
                && Prefix::ATTRIBUTE_MAX.prefix_id().bytes() >= prefix.prefix_id().bytes
        );
        let mut bytes = ByteArray::zeros(Self::RANGE_PREFIX.end + attribute_vertex_prefix.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + attribute_vertex_prefix.length()]
            .copy_from_slice(attribute_vertex_prefix.bytes());
        StorageKey::new_owned(Self::keyspace_for_from_prefix(prefix), bytes)
    }

    // TODO cleanup
    pub fn prefix_from_attribute(
        from: AttributeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_BOUND_PREFIX_FROM);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from_for_vertex(from.as_reference())].copy_from_slice(from.bytes().bytes());
        bytes.truncate(Self::range_from_for_vertex(from.as_reference()).end);
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_to_type(
        from: AttributeVertex<'_>,
        to_type: TypeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_BOUND_PREFIX_FROM_TO_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        let range_from = Self::range_from_for_vertex(from.as_reference());
        bytes.bytes_mut()[range_from.clone()].copy_from_slice(from.bytes().bytes());
        let to_type_range = range_from.end..range_from.end + TypeVertex::LENGTH;
        bytes.bytes_mut()[to_type_range].copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(to_type).bytes());
        bytes.truncate(range_from.end + TypeVertex::LENGTH);
        StorageKey::new_owned(EncodingKeyspace::Data, bytes)
    }

    pub fn prefix_from_attribute_to_type_range(
        from: AttributeVertex<'_>,
        range_start: TypeVertex<'_>,
        range_end: RangeEnd<TypeVertex<'_>>,
    ) -> KeyRange<StorageKey<'static, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }>> {
        KeyRange::new_fixed_width(
            RangeStart::Inclusive(Self::prefix_from_attribute_to_type(from.clone(), range_start)),
            range_end.map(|vertex| Self::prefix_from_attribute_to_type(from, vertex)),
        )
    }
    // end TODO

    pub fn is_has_reverse(key: StorageKeyReference<'_>) -> bool {
        if !key.bytes().is_empty() && key.bytes()[Self::RANGE_PREFIX] == Self::PREFIX.prefix_id().bytes() {
            let edge = ThingEdgeHasReverse::new(Bytes::Reference(key.byte_ref()));
            edge.keyspace().id() == key.keyspace_id()
        } else {
            false
        }
    }

    pub fn from(&'a self) -> AttributeVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[self.range_from()]);
        AttributeVertex::new(Bytes::Reference(reference))
    }

    pub fn to(&'a self) -> ObjectVertex<'a> {
        let reference = ByteReference::new(&self.bytes.bytes()[self.range_to()]);
        ObjectVertex::new(Bytes::Reference(reference))
    }

    pub fn into_from(self) -> AttributeVertex<'a> {
        let range = self.range_from();
        AttributeVertex::new(self.bytes.into_range(range))
    }

    pub fn into_to(self) -> ObjectVertex<'a> {
        let range = self.range_to();
        ObjectVertex::new(self.bytes.into_range(range))
    }

    fn range_from(&self) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + self.from_length()
    }

    #[allow(clippy::wrong_self_convention, reason = "`from` refers to the edge's source vertex")]
    fn from_length(&self) -> usize {
        let value_type_prefix = self.bytes.bytes()[Self::INDEX_FROM_VALUE_PREFIX];
        let id_encoding_length =
            AttributeID::value_type_encoding_length(ValueTypeCategory::from_bytes([value_type_prefix]));
        THING_VERTEX_LENGTH_PREFIX_TYPE + id_encoding_length
    }

    fn range_to(&self) -> Range<usize> {
        self.range_from().end..self.length()
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    fn range_from_for_vertex(from: AttributeVertex) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + from.length()
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeHasReverse<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ThingEdgeHasReverse<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeHasReverse<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Data
    }
}

///
/// [rp][relation][object][role_id]
/// OR
/// [rp_reverse][object][relation][role_id]
///
pub struct ThingEdgeLinks<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeLinks<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeLinks;
    const PREFIX_REVERSE: Prefix = Prefix::EdgeLinksReverse;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();
    pub const FIXED_WIDTH_ENCODING_REVERSE: bool = Self::PREFIX_REVERSE.fixed_width_keys();

    const RANGE_FROM: Range<usize> = Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + ObjectVertex::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + ObjectVertex::LENGTH;
    const RANGE_ROLE_ID: Range<usize> = Self::RANGE_TO.end..Self::RANGE_TO.end + TypeID::LENGTH;
    const LENGTH: usize = PrefixID::LENGTH + 2 * ObjectVertex::LENGTH + TypeID::LENGTH;
    pub const LENGTH_PREFIX_FROM_TYPE: usize = PrefixID::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;
    pub const LENGTH_PREFIX_FROM_TO_TYPE: usize =
        PrefixID::LENGTH + ObjectVertex::LENGTH + THING_VERTEX_LENGTH_PREFIX_TYPE;
    pub const LENGTH_PREFIX_FROM_TO: usize = PrefixID::LENGTH + ObjectVertex::LENGTH + ObjectVertex::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let edge = ThingEdgeLinks { bytes };
        debug_assert!(edge.prefix() == Self::PREFIX || edge.prefix() == Self::PREFIX_REVERSE);
        edge
    }

    pub fn build_links(relation: ObjectVertex<'_>, player: ObjectVertex<'_>, role_type: TypeVertex<'_>) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(relation.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TO].copy_from_slice(player.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_ROLE_ID].copy_from_slice(&role_type.type_id_().bytes());
        ThingEdgeLinks { bytes: Bytes::Array(bytes) }
    }

    pub fn build_links_reverse(
        player: ObjectVertex<'_>,
        relation: ObjectVertex<'_>,
        role_type: TypeVertex<'_>,
    ) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX_REVERSE.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(player.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TO].copy_from_slice(relation.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_ROLE_ID].copy_from_slice(&role_type.type_id_().bytes());
        ThingEdgeLinks { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from_relation_type(
        relation_type: TypeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from_type()]
            .copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(relation_type).bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation(
        relation: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(relation.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation_player_type(
        relation: ObjectVertex<'_>,
        player_type: TypeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(relation.bytes().bytes());
        bytes.bytes_mut()[Self::range_to_type()]
            .copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(player_type).bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_from_relation_player(
        relation: ObjectVertex<'_>,
        player: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(relation.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TO].copy_from_slice(player.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player(
        player: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX_REVERSE.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(player.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_type(
        player_type: TypeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX_REVERSE.prefix_id().bytes());
        bytes.bytes_mut()[Self::range_from_type()]
            .copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(player_type).bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_relation_type(
        player: ObjectVertex<'_>,
        relation_type: TypeVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX_REVERSE.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(player.bytes().bytes());
        bytes.bytes_mut()[Self::range_to_type()]
            .copy_from_slice(ObjectVertex::build_prefix_from_type_vertex(relation_type).bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix_reverse_from_player_relation(
        player: ObjectVertex<'_>,
        relation: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeLinks::LENGTH_PREFIX_FROM_TO }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM_TO);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX_REVERSE.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(player.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TO].copy_from_slice(relation.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn prefix() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX.prefix_id().bytes()))
    }

    pub fn prefix_reverse() -> StorageKey<'static, { PrefixID::LENGTH }> {
        StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(&Self::PREFIX_REVERSE.prefix_id().bytes()))
    }

    const fn range_from_type() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + THING_VERTEX_LENGTH_PREFIX_TYPE
    }

    const fn range_to_type() -> Range<usize> {
        Self::LENGTH_PREFIX_FROM..Self::LENGTH_PREFIX_FROM + THING_VERTEX_LENGTH_PREFIX_TYPE
    }

    pub fn is_links(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::RANGE_PREFIX] == Self::PREFIX.prefix_id().bytes()
    }

    pub fn is_links_reverse(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::RANGE_PREFIX] == Self::PREFIX_REVERSE.prefix_id().bytes()
    }

    pub fn from(&self) -> ObjectVertex<'_> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_FROM])))
    }

    pub fn to(&self) -> ObjectVertex<'_> {
        // TODO: copy?
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes.bytes()[Self::RANGE_TO])))
    }

    pub fn into_from(self) -> ObjectVertex<'a> {
        ObjectVertex::new(self.bytes.into_range(Self::RANGE_FROM))
    }

    pub fn into_to(self) -> ObjectVertex<'a> {
        ObjectVertex::new(self.bytes.into_range(Self::RANGE_TO))
    }

    pub fn relation(&self) -> ObjectVertex<'_> {
        if self.is_reverse() {
            self.to()
        } else {
            self.from()
        }
    }

    pub fn player(&self) -> ObjectVertex<'_> {
        if self.is_reverse() {
            self.from()
        } else {
            self.to()
        }
    }

    pub fn into_relation(self) -> ObjectVertex<'a> {
        if self.is_reverse() {
            self.into_to()
        } else {
            self.into_from()
        }
    }

    pub fn into_player(self) -> ObjectVertex<'a> {
        if self.is_reverse() {
            self.into_from()
        } else {
            self.into_to()
        }
    }

    fn is_reverse(&self) -> bool {
        self.bytes().bytes()[Self::RANGE_PREFIX] == Self::PREFIX_REVERSE.prefix_id().bytes()
    }

    pub fn role_id(&'a self) -> TypeID {
        let bytes = &self.bytes.bytes()[Self::RANGE_ROLE_ID];
        TypeID::new(bytes.try_into().unwrap())
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeLinks<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ThingEdgeLinks<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeLinks<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

///
/// [rp_index][from_object][to_object][relation][from_role_id][to_role_id]
///
pub struct ThingEdgeRolePlayerIndex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ThingEdgeRolePlayerIndex<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::EdgeRolePlayerIndex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const RANGE_FROM: Range<usize> = Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + ObjectVertex::LENGTH;
    const RANGE_TO: Range<usize> = Self::RANGE_FROM.end..Self::RANGE_FROM.end + ObjectVertex::LENGTH;
    const RANGE_RELATION: Range<usize> = Self::RANGE_TO.end..Self::RANGE_TO.end + ObjectVertex::LENGTH;
    const RANGE_FROM_ROLE_TYPE_ID: Range<usize> = Self::RANGE_RELATION.end..Self::RANGE_RELATION.end + TypeID::LENGTH;
    const RANGE_TO_ROLE_TYPE_ID: Range<usize> =
        Self::RANGE_FROM_ROLE_TYPE_ID.end..Self::RANGE_FROM_ROLE_TYPE_ID.end + TypeID::LENGTH;
    const LENGTH: usize = PrefixID::LENGTH + 3 * ObjectVertex::LENGTH + 2 * TypeID::LENGTH;
    pub const LENGTH_PREFIX_FROM: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        let index = ThingEdgeRolePlayerIndex { bytes };
        debug_assert_eq!(index.prefix(), Self::PREFIX);
        index
    }

    pub fn build(
        from: ObjectVertex<'_>,
        to: ObjectVertex<'_>,
        relation: ObjectVertex<'_>,
        from_role_type_id: TypeID,
        to_role_type_id: TypeID,
    ) -> ThingEdgeRolePlayerIndex<'static> {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(from.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TO].copy_from_slice(to.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_RELATION].copy_from_slice(relation.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM_ROLE_TYPE_ID].copy_from_slice(&from_role_type_id.bytes());
        bytes.bytes_mut()[Self::RANGE_TO_ROLE_TYPE_ID].copy_from_slice(&to_role_type_id.bytes());
        ThingEdgeRolePlayerIndex { bytes: Bytes::Array(bytes) }
    }

    pub fn prefix_from(
        from: ObjectVertex<'_>,
    ) -> StorageKey<'static, { ThingEdgeRolePlayerIndex::LENGTH_PREFIX_FROM }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_FROM);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_FROM].copy_from_slice(from.bytes().bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn is_index(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == Self::KEYSPACE.id()
            && key.bytes().len() == Self::LENGTH
            && key.bytes()[Self::RANGE_PREFIX] == Self::PREFIX.prefix_id().bytes()
    }

    pub fn from(&self) -> ObjectVertex<'_> {
        Self::read_from(self.bytes())
    }

    pub fn read_from(reference: ByteReference<'_>) -> ObjectVertex<'static> {
        // TODO: copy?
        ObjectVertex::new(Bytes::copy(&reference.bytes()[Self::RANGE_FROM]))
    }

    pub fn to(&self) -> ObjectVertex<'_> {
        // TODO: copy?
        Self::read_to(self.bytes())
    }

    pub fn read_to(reference: ByteReference<'_>) -> ObjectVertex<'static> {
        ObjectVertex::new(Bytes::copy(&reference.bytes()[Self::RANGE_TO]))
    }

    pub fn relation(&self) -> ObjectVertex<'_> {
        Self::read_relation(self.bytes())
    }

    pub fn read_relation(reference: ByteReference) -> ObjectVertex<'static> {
        ObjectVertex::new(Bytes::copy(&reference.bytes()[Self::RANGE_RELATION]))
    }

    pub fn from_role_id(&self) -> TypeID {
        Self::read_from_role_id(self.bytes())
    }

    pub fn read_from_role_id(reference: ByteReference) -> TypeID {
        TypeID::new(reference.bytes()[Self::RANGE_FROM_ROLE_TYPE_ID].try_into().unwrap())
    }

    pub fn to_role_id(&self) -> TypeID {
        Self::read_to_role_id(self.bytes())
    }

    pub fn read_to_role_id(reference: ByteReference) -> TypeID {
        TypeID::new(reference.bytes()[Self::RANGE_TO_ROLE_TYPE_ID].try_into().unwrap())
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ThingEdgeRolePlayerIndex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}
