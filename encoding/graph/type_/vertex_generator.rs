/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::iter::zip;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use primitive::prefix_range::PrefixRange;

use storage::{snapshot::WriteSnapshot, MVCCStorage};
use storage::key_value::{StorageKey, StorageKeyArray, StorageKeyReference};
use storage::keyspace::iterator::KeyspaceRangeIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    graph::type_::vertex::{
        build_vertex_attribute_type, build_vertex_attribute_type_prefix, build_vertex_entity_type,
        build_vertex_entity_type_prefix, build_vertex_relation_type, build_vertex_relation_type_prefix,
        build_vertex_role_type, build_vertex_role_type_prefix, TypeID, TypeVertex,
    },
    Keyable,
};
use crate::error::EncodingError;
use crate::error::EncodingErrorKind::FailedTypeIDAllocation;
use crate::graph::type_::vertex::TypeIDUInt;

// TODO: if we always scan for the next available TypeID, we automatically recycle deleted TypeIDs?
//          -> If we do reuse TypeIDs, this we also need to make sure to reset the Thing ID generators on delete! (test should exist to confirm this).

pub struct TypeIDAllocator<const PREFIX_LENGTH: usize> {
    prefix: PrefixRange<StorageKeyArray<PREFIX_LENGTH>>
}

impl<const PREFIX_LENGTH: usize> TypeIDAllocator<PREFIX_LENGTH> {

    fn new(prefix: PrefixRange<StorageKeyArray<PREFIX_LENGTH>>) -> TypeIDAllocator<PREFIX_LENGTH> {
        Self { prefix }
    }

    fn extract_type_id(key: StorageKeyReference) -> TypeIDUInt {
        let bytes = key.bytes();
        TypeIDUInt::from_be_bytes([bytes[bytes.len()-2], bytes[bytes.len()-1]])
    }

    fn allocate(&self, snapshot: &dyn WritableSnapshot) ->  Result<TypeIDUInt, EncodingError> {
        let mut type_id_iter = snapshot.iterate_range(self.prefix.into());
        for expected_next in (0..=u16::MAX) {
            if let Some(next_res) = type_id_iter.next() {
                if Self::extract_type_id(next_res?.0) != expected_next { Ok(expected_next) }
            } else {
                Err(EncodingError{ kind: FailedTypeIDAllocation })
            }
        }
        Err(EncodingError{ kind: FailedTypeIDAllocation })
    }
}

pub struct TypeVertexGenerator {
    next_entity: TypeIDAllocator<1>,
    next_relation: TypeIDAllocator<1>,
    next_role: TypeIDAllocator<1>,
    next_attribute: TypeIDAllocator<1>,
}

impl Default for TypeVertexGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl<const PREFIX_LENGTH: usize> TypeVertexGenerator {
    const U16_LENGTH: usize = std::mem::size_of::<u16>();

    pub fn new() -> TypeVertexGenerator {
        TypeVertexGenerator {
            next_entity: TypeIDAllocator::new(PrefixRange::new_within(build_vertex_entity_type_prefix().into_owned_array())),
            next_relation: TypeIDAllocator::new(PrefixRange::new_within(build_vertex_relation_type_prefix().into_owned_array())),
            next_role: TypeIDAllocator::new(PrefixRange::new_within(build_vertex_role_type_prefix().into_owned_array())),
            next_attribute: TypeIDAllocator::new(PrefixRange::new_within(build_vertex_attribute_type_prefix().into_owned_array())),
        }
    }

    pub fn create_entity_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_entity.allocate(snapshot).unwrap()); // TODO: Error handling
        let vertex = build_vertex_entity_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_relation.allocate(snapshot).unwrap()); // TODO: Error handling
        let vertex = build_vertex_relation_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_role.allocate(snapshot).unwrap()); // TODO: Error handling
        let vertex = build_vertex_role_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_attribute.allocate(snapshot).unwrap()); // TODO: Error handling
        let vertex = build_vertex_attribute_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }
}
