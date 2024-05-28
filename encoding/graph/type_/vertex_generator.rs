/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU16, Ordering::Relaxed};

use bytes::Bytes;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::WritableSnapshot};

use crate::{
    error::EncodingError,
    graph::{
        type_::{
            vertex::{
                TypeID, TypeIDUInt, TypeVertex,
            },
            Kind,
            Kind::{Attribute, Entity, Relation, Role},
        },
        Typed,
    },
    Keyable,
};
use crate::layout::prefix::Prefix;

pub struct TypeVertexAllocator {
    kind: Kind,
    last_allocated_type_id: AtomicU16,
    prefix: Prefix,
}

impl TypeVertexAllocator {
    fn new(kind: Kind, prefix: Prefix) -> TypeVertexAllocator {
        Self { kind, last_allocated_type_id: AtomicU16::new(0), prefix }
    }

    fn find_unallocated_id<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &Snapshot,
        start: TypeIDUInt,
    ) -> Result<Option<TypeIDUInt>, EncodingError> {
        let mut type_vertex_iter = snapshot.iterate_range(KeyRange::new_inclusive(
            TypeVertex::build(self.prefix.prefix_id(), TypeID::build(start)).into_storage_key(),
            TypeVertex::build(self.prefix.prefix_id(), TypeID::build(TypeIDUInt::MAX)).into_storage_key(),
        ));
        for expected_next in start..=TypeIDUInt::MAX {
            match type_vertex_iter.next() {
                None => return Ok(Some(expected_next)),
                Some(Err(err)) => {
                    return Err(EncodingError::TypeIDAllocate { source: err.clone() });
                }
                Some(Ok((actual_next_key, _))) => {
                    let actual_type_id = TypeVertex::new(Bytes::reference(actual_next_key.bytes())).type_id_().as_u16();
                    if actual_type_id != expected_next {
                        return Ok(Some(expected_next));
                    }
                }
            }
        }
        Ok(None)
    }

    fn allocate<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let found = self.find_unallocated_id(snapshot, self.last_allocated_type_id.load(Relaxed))?;
        if let Some(type_id) = found {
            self.last_allocated_type_id.store(type_id, Relaxed);
            Ok(TypeVertex::build(self.prefix.prefix_id(), TypeID::build(type_id)))
        } else {
            match self.find_unallocated_id(snapshot, 0)? {
                None => Err(EncodingError::TypeIDsExhausted { kind: self.kind }),
                Some(type_id) => {
                    self.last_allocated_type_id.store(type_id, Relaxed);
                    Ok(TypeVertex::build(self.prefix.prefix_id(), TypeID::build(type_id)))
                }
            }
        }
    }
}

pub struct TypeVertexGenerator {
    next_entity: TypeVertexAllocator,
    next_relation: TypeVertexAllocator,
    next_role: TypeVertexAllocator,
    next_attribute: TypeVertexAllocator,
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
            next_entity: TypeVertexAllocator::new(Entity, Prefix::VertexEntityType),
            next_relation: TypeVertexAllocator::new(Relation, Prefix::VertexRelationType),
            next_role: TypeVertexAllocator::new(Role, Prefix::VertexRoleType),
            next_attribute: TypeVertexAllocator::new(Attribute, Prefix::VertexAttributeType),
        }
    }

    pub fn create_entity_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_entity.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_relation.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_role.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_attribute.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }
}
