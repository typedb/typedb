/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use self::{vertex_attribute::AttributeVertex, vertex_object::ObjectVertex};
use crate::{
    graph::{type_::vertex::TypeID, Typed},
    layout::prefix::{Prefix, PrefixID},
    EncodingKeyspace, Keyable, Prefixed,
};

pub mod edge;
pub mod property;
pub mod vertex_attribute;
pub mod vertex_generator;
pub mod vertex_object;

const THING_VERTEX_LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

const fn max(lhs: usize, rhs: usize) -> usize {
    if lhs < rhs {
        rhs
    } else {
        lhs
    }
}

pub const THING_VERTEX_MAX_LENGTH: usize = max(ObjectVertex::LENGTH, AttributeVertex::MAX_LENGTH);

pub trait ThingVertex: Prefixed<BUFFER_KEY_INLINE> + Typed<BUFFER_KEY_INLINE> + Keyable<BUFFER_KEY_INLINE> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;

    fn new(bytes: &[u8]) -> Self;

    fn build_prefix_prefix(prefix: Prefix) -> StorageKey<'static, { PrefixID::LENGTH }> {
        debug_assert!(matches!(prefix, Prefix::VertexEntity | Prefix::VertexRelation | Prefix::VertexAttribute));
        let mut array = ByteArray::zeros(PrefixID::LENGTH);
        array[Self::INDEX_PREFIX] = prefix.prefix_id().byte;
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }

    fn build_prefix_type(prefix: Prefix, type_id: TypeID) -> StorageKey<'static, THING_VERTEX_LENGTH_PREFIX_TYPE> {
        debug_assert!(matches!(prefix, Prefix::VertexEntity | Prefix::VertexRelation | Prefix::VertexAttribute));
        let mut array = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE);
        Self::write_prefix_type(array.as_mut(), prefix, type_id);
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }

    fn write_prefix_type(bytes: &mut [u8], prefix: Prefix, type_id: TypeID) -> usize {
        debug_assert!(bytes.len() >= THING_VERTEX_LENGTH_PREFIX_TYPE);
        bytes[Self::INDEX_PREFIX] = prefix.prefix_id().byte;
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.to_bytes());
        THING_VERTEX_LENGTH_PREFIX_TYPE
    }
}
