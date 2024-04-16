/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use primitive::prefix_range::PrefixRange;

use storage::snapshot::WritableSnapshot;

use crate::{
    graph::type_::vertex::{
        build_vertex_attribute_type, build_vertex_entity_type, build_vertex_relation_type,
        build_vertex_role_type, TypeID, TypeVertex,
    },
    Keyable,
};
use crate::error::EncodingError;
use crate::error::EncodingErrorKind::{ExhaustedTypeIDs, FailedTypeIDAllocation};
use crate::graph::type_::Kind;
use crate::graph::type_::Kind::{Attribute, Entity, Relation, Role};
use crate::graph::type_::vertex::TypeIDUInt;
use crate::graph::Typed;

pub struct TypeIDAllocator {
    kind: Kind,
    last_allocated_type_id: AtomicU16,
    vertex_constructor:  fn (TypeID) -> TypeVertex<'static>
}

impl TypeIDAllocator {

    fn new(root_type: Kind, to_vertex: fn (TypeID) -> TypeVertex<'static>) -> TypeIDAllocator {
        Self { kind: root_type, last_allocated_type_id : AtomicU16::new(0), vertex_constructor: to_vertex }
    }

    fn find_unallocated<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot, start: TypeIDUInt) -> Result<Option<TypeIDUInt>, EncodingError> {
        let mut type_vertex_iter = snapshot.iterate_range(
            PrefixRange::new_inclusive(
                (self.vertex_constructor)(TypeID::build(start)).into_storage_key(),
                (self.vertex_constructor)(TypeID::build(TypeIDUInt::MAX)).into_storage_key()
            )
        );
        for expected_next in start..=TypeIDUInt::MAX {
            match type_vertex_iter.next() {
                None => {return Ok(Some(expected_next))},
                Some(Err(err)) => {return Err(EncodingError{ kind: FailedTypeIDAllocation { source: err.clone() } });},
                Some(Ok((actual_next_key, _))) => {
                    let actual_type_id = TypeVertex::new(Bytes::Reference(actual_next_key.byte_ref())).type_id_().as_u16();
                    if actual_type_id != expected_next { return Ok(Some(expected_next)); }
                }
            }
        }
        Ok(None)
    }

    fn allocate<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) ->  Result<TypeVertex<'static>, EncodingError> {
        let found = self.find_unallocated(snapshot, self.last_allocated_type_id.load(Relaxed))?;
        if let(Some(type_id)) = found {
            self.last_allocated_type_id.store(type_id, Relaxed);
            Ok((self.vertex_constructor)(TypeID::build(type_id)))
        } else {
            match self.find_unallocated(snapshot, 0)? {
                None => Err(EncodingError{ kind: ExhaustedTypeIDs{ root_type : self.kind } }),
                Some(type_id) => {
                    self.last_allocated_type_id.store(type_id, Relaxed);
                    Ok((self.vertex_constructor)(TypeID::build(type_id)))
                }
            }
        }
    }
}

pub struct TypeVertexGenerator {
    next_entity: TypeIDAllocator,
    next_relation: TypeIDAllocator,
    next_role: TypeIDAllocator,
    next_attribute: TypeIDAllocator,
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
            next_entity: TypeIDAllocator::new(Entity, build_vertex_entity_type),
            next_relation: TypeIDAllocator::new(Relation, build_vertex_relation_type),
            next_role: TypeIDAllocator::new(Role, build_vertex_role_type),
            next_attribute: TypeIDAllocator::new(Attribute, build_vertex_attribute_type),
        }
    }

    pub fn create_entity_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_entity.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_relation.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_role.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let vertex = self.next_attribute.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }
}
