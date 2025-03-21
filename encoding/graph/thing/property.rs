/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::{thing::vertex_object::ObjectVertex, type_::vertex::TypeVertex, Typed},
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub fn build_object_vertex_property_has_order(
    object_vertex: ObjectVertex,
    attribute_type: TypeVertex,
) -> ObjectVertexProperty {
    debug_assert_eq!(attribute_type.prefix(), Prefix::VertexAttributeType);
    let suffix: Bytes<'_, BUFFER_KEY_INLINE> = Bytes::Array(ByteArray::copy(&attribute_type.type_id_().to_bytes()));
    ObjectVertexProperty::new_suffixed(object_vertex, Infix::PropertyHasOrder, suffix)
}

pub fn build_object_vertex_property_links_order(
    object_vertex: ObjectVertex,
    role_type: TypeVertex,
) -> ObjectVertexProperty {
    debug_assert_eq!(role_type.prefix(), Prefix::VertexRoleType);
    let suffix: Bytes<'_, BUFFER_KEY_INLINE> = Bytes::Array(ByteArray::copy(&role_type.type_id_().to_bytes()));
    ObjectVertexProperty::new_suffixed(object_vertex, Infix::PropertyLinksOrder, suffix)
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ObjectVertexProperty {
    object: ObjectVertex,
    infix: Infix,
    suffix: Option<ByteArray<0>>,
}

impl ObjectVertexProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyObjectVertex;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + ObjectVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    fn new(object: ObjectVertex, infix: Infix) -> Self {
        Self { object, infix, suffix: None }
    }

    fn new_suffixed<const INLINE_BYTES: usize>(
        object: ObjectVertex,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        Self { object, infix, suffix: Some(ByteArray::copy(&suffix)) }
    }

    pub fn build_prefix() -> StorageKey<'static, { ObjectVertexProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = ObjectVertexProperty::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn object_vertex(&self) -> ObjectVertex {
        self.object
    }

    pub fn infix(&self) -> Infix {
        self.infix
    }

    fn suffix_length(&self) -> usize {
        self.suffix.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    pub fn suffix(&self) -> Option<&[u8]> {
        self.suffix.as_deref()
    }

    const fn range_object_vertex() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + ObjectVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_object_vertex().end..Self::range_object_vertex().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for ObjectVertexProperty {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + self.suffix_length());
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_object_vertex()].copy_from_slice(&self.object.to_bytes());
        array[Self::range_infix()].copy_from_slice(&self.infix.infix_id().bytes());
        if let Some(suffix) = self.suffix() {
            array[Self::range_suffix(suffix.len())].copy_from_slice(suffix);
        }
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for ObjectVertexProperty {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for ObjectVertexProperty {}
