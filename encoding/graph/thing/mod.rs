/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    EncodingKeyspace,
    graph::{type_::vertex::TypeID, Typed},
    layout::prefix::{Prefix, PrefixID}, Prefixed,
};

pub mod edge;
pub mod property;
pub mod vertex_attribute;
pub mod vertex_generator;
pub mod vertex_object;

const THING_VERTEX_LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

pub trait ThingVertex<'a>: Prefixed<'a, BUFFER_KEY_INLINE> + Typed<'a, BUFFER_KEY_INLINE> {
    const KEYSPACE: EncodingKeyspace;

    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self;

    fn build_prefix_prefix(prefix: Prefix) -> StorageKey<'static, { PrefixID::LENGTH }> {
        let mut array = ByteArray::zeros(PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }

    fn build_prefix_type(prefix: Prefix, type_id: TypeID) -> StorageKey<'static, THING_VERTEX_LENGTH_PREFIX_TYPE> {
        debug_assert!(
            prefix == Prefix::VertexEntity
                || prefix == Prefix::VertexRelation
                || (prefix.prefix_id().bytes() >= Prefix::ATTRIBUTE_MIN.prefix_id().bytes
                    && prefix.prefix_id().bytes() < Prefix::ATTRIBUTE_MAX.prefix_id().bytes)
        );
        let mut array = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }
}
