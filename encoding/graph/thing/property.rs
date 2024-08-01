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
    graph::{
        thing::{vertex_object::ObjectVertex, ThingVertex},
        type_::vertex::TypeVertex,
        Typed,
    },
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

trait ObjectVertexPropertyFactory {
    fn infix(&self) -> Infix;

    fn create<'a>(&self, bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> ObjectVertexProperty<'a> {
        let property = ObjectVertexProperty::new(bytes);
        debug_assert_eq!(property.infix(), self.infix());
        property
    }

    fn matches(&self, bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        Prefix::from_prefix_id(PrefixID::new([bytes.bytes()[0]])) == ObjectVertexProperty::PREFIX
            && self.create(bytes).infix() == self.infix()
    }
}

pub struct HasOrderPropertyFactory {}

impl HasOrderPropertyFactory {
    pub fn build(&self, object_vertex: ObjectVertex, attribute_type: TypeVertex) -> ObjectVertexProperty<'static> {
        debug_assert_eq!(attribute_type.prefix(), Prefix::VertexAttributeType);
        let suffix: Bytes<'_, BUFFER_KEY_INLINE> = Bytes::Array(ByteArray::copy(&attribute_type.type_id_().bytes()));
        ObjectVertexProperty::build_suffixed(object_vertex, self.infix(), suffix)
    }
}

impl ObjectVertexPropertyFactory for HasOrderPropertyFactory {
    fn infix(&self) -> Infix {
        Infix::PropertyHasOrder
    }
}

pub const HAS_ORDER_PROPERTY_FACTORY: HasOrderPropertyFactory = HasOrderPropertyFactory {};

pub struct RolePlayerOrderPropertyFactory {}

impl RolePlayerOrderPropertyFactory {
    pub fn build(&self, object_vertex: ObjectVertex, role_type: TypeVertex) -> ObjectVertexProperty<'static> {
        debug_assert_eq!(role_type.prefix(), Prefix::VertexRoleType);
        let suffix: Bytes<'_, BUFFER_KEY_INLINE> = Bytes::Array(ByteArray::copy(&role_type.type_id_().bytes()));
        ObjectVertexProperty::build_suffixed(object_vertex, self.infix(), suffix)
    }
}

impl ObjectVertexPropertyFactory for RolePlayerOrderPropertyFactory {
    fn infix(&self) -> Infix {
        Infix::PropertyRolePlayerOrder
    }
}

pub const ROLE_PLAYER_ORDER_PROPERTY_FACTORY: RolePlayerOrderPropertyFactory = RolePlayerOrderPropertyFactory {};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ObjectVertexProperty<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ObjectVertexProperty<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const PREFIX: Prefix = Prefix::PropertyObjectVertex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + ObjectVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + ObjectVertex::LENGTH;

    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = ObjectVertexProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    fn build(vertex: ObjectVertex<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_object_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        ObjectVertexProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        vertex: ObjectVertex<'_>,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_object_vertex()].copy_from_slice(vertex.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array.bytes_mut()[Self::range_suffix(suffix.length())].copy_from_slice(suffix.bytes());
        ObjectVertexProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { ObjectVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = ObjectVertexProperty::PREFIX.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, ByteReference::new(&PREFIX_BYTES))
    }

    pub fn object_vertex(&'a self) -> ObjectVertex<'a> {
        ObjectVertex::new(Bytes::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_object_vertex()])))
    }

    pub fn infix(&self) -> Infix {
        let infix_bytes = &self.bytes.bytes()[Self::range_infix()];
        Infix::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    fn suffix_length(&self) -> usize {
        self.bytes().length() - Self::LENGTH_NO_SUFFIX
    }

    pub fn suffix(&self) -> Option<ByteReference> {
        let suffix_length = self.suffix_length();
        if suffix_length > 0 {
            Some(ByteReference::new(&self.bytes.bytes()[Self::range_suffix(self.suffix_length())]))
        } else {
            None
        }
    }

    const fn range_object_vertex() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + ObjectVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_object_vertex().end..Self::range_object_vertex().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ObjectVertexProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ObjectVertexProperty<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ObjectVertexProperty<'a> {}
