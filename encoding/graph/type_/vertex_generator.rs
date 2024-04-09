/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU16, Ordering};

use storage::{snapshot::WriteSnapshot, MVCCStorage};
use storage::snapshot::WritableSnapshot;

use crate::{
    graph::type_::vertex::{
        build_vertex_attribute_type, build_vertex_attribute_type_prefix, build_vertex_entity_type,
        build_vertex_entity_type_prefix, build_vertex_relation_type, build_vertex_relation_type_prefix,
        build_vertex_role_type, build_vertex_role_type_prefix, TypeID, TypeVertex,
    },
    Keyable,
};

// TODO: if we always scan for the next available TypeID, we automatically recycle deleted TypeIDs?
//          -> If we do reuse TypeIDs, this we also need to make sure to reset the Thing ID generators on delete! (test should exist to confirm this).
pub struct TypeVertexGenerator {
    next_entity: AtomicU16,
    next_relation: AtomicU16,
    next_role: AtomicU16,
    next_attribute: AtomicU16,
}

impl Default for TypeVertexGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeVertexGenerator {
    const U16_LENGTH: usize = std::mem::size_of::<u16>();

    pub fn new() -> TypeVertexGenerator {
        TypeVertexGenerator {
            next_entity: AtomicU16::new(0),
            next_relation: AtomicU16::new(0),
            next_role: AtomicU16::new(0),
            next_attribute: AtomicU16::new(0),
        }
    }

    pub fn load<D>(storage: &MVCCStorage<D>) -> TypeVertexGenerator {
        let next_entity: AtomicU16 = storage
            .get_prev_raw(build_vertex_entity_type_prefix().as_reference(), |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            })
            .unwrap_or_else(|| AtomicU16::new(0));
        let next_relation: AtomicU16 = storage
            .get_prev_raw(build_vertex_relation_type_prefix().as_reference(), |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            })
            .unwrap_or_else(|| AtomicU16::new(0));
        let next_role: AtomicU16 = storage
            .get_prev_raw(build_vertex_role_type_prefix().as_reference(), |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            })
            .unwrap_or_else(|| AtomicU16::new(0));
        let next_attribute: AtomicU16 = storage
            .get_prev_raw(build_vertex_attribute_type_prefix().as_reference(), |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            })
            .unwrap_or_else(|| AtomicU16::new(0));
        TypeVertexGenerator { next_entity, next_relation, next_role, next_attribute }
    }

    pub fn create_entity_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_entity.fetch_add(1, Ordering::Relaxed));
        let vertex = build_vertex_entity_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_relation.fetch_add(1, Ordering::Relaxed));
        let vertex = build_vertex_relation_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_role.fetch_add(1, Ordering::Relaxed));
        let vertex = build_vertex_role_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> TypeVertex<'static> {
        let next = TypeID::build(self.next_attribute.fetch_add(1, Ordering::Relaxed));
        let vertex = build_vertex_attribute_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }
}
