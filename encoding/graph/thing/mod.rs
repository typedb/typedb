/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;
use bytes::byte_array::ByteArray;
use bytes::Bytes;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;
use crate::graph::type_::vertex::{TypeID, TypeVertex};
use crate::layout::prefix::{Prefix, PrefixID};
use crate::{EncodingKeyspace, Prefixed};
use crate::graph::thing::vertex_object::ObjectVertex;
use crate::graph::Typed;

pub mod edge;
pub mod property;
pub mod vertex_attribute;
pub mod vertex_generator;
pub mod vertex_object;


const THING_VERTEX_LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

pub trait ThingVertex<'a>: Prefixed<'a, BUFFER_KEY_INLINE> + Typed<'a, BUFFER_KEY_INLINE> {
    const KEYSPACE: EncodingKeyspace;

    fn build_prefix_prefix(prefix: Prefix) -> StorageKey<'static, THING_VERTEX_LENGTH_PREFIX_TYPE> {
        let mut array = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }

    fn build_prefix_type(
        prefix: Prefix,
        type_id: TypeID,
    ) -> StorageKey<'static, THING_VERTEX_LENGTH_PREFIX_TYPE> {
        debug_assert!(prefix == Prefix::VertexEntity || prefix == Prefix::VertexRelation);
        let mut array = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.prefix_id().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }
}