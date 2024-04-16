/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use storage::key_value::{StorageKey, StorageKeyReference};
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
    root_type : Kind,
    last_allocated_type_id: AtomicU16,
    to_vertex:  fn (TypeID) -> TypeVertex<'static>
}

impl TypeIDAllocator {

    fn new(root_type: Kind, to_vertex: fn (TypeID) -> TypeVertex<'static>) -> TypeIDAllocator {
        Self { root_type, last_allocated_type_id : AtomicU16::new(0), to_vertex }
    }

    fn to_key(&self, type_id: TypeIDUInt) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        (self.to_vertex)(TypeID::build(type_id)).into_storage_key()
    }

    fn to_type_id<'a>(&self, key : StorageKeyReference) -> TypeIDUInt {
        TypeVertex::new(Bytes::Reference(key.byte_ref())).type_id_().as_u16()
    }

    fn iterate_and_find<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot, start: TypeIDUInt) -> Result<Option<TypeIDUInt>, EncodingError> {
        let mut type_id_iter = snapshot.iterate_range(PrefixRange::new_inclusive(self.to_key(start), self.to_key(TypeIDUInt::MAX)));
        for expected_next in (start..=TypeIDUInt::MAX) {
            match type_id_iter.next() {
                None => {return Ok(Some(expected_next))},
                Some(Err(err)) => {return Err(EncodingError{ kind: FailedTypeIDAllocation { source: err.clone() } });},
                Some(Ok((actual_next, _))) => {
                    if self.to_type_id(actual_next) != expected_next { return Ok(Some(expected_next)); }
                }
            }
        }
        Ok(None)
    }

    fn allocate<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) ->  Result<TypeIDUInt, EncodingError> {
        let found = self.iterate_and_find(snapshot, self.last_allocated_type_id.load(Relaxed))?;
        if let(Some(type_id)) = found {
            self.last_allocated_type_id.store(type_id, Relaxed);
            Ok(type_id)
        } else {
            match self.iterate_and_find(snapshot, 0)? {
                None => Err(EncodingError{ kind: ExhaustedTypeIDs{ root_type : self.root_type } }),
                Some(type_id) => {
                    self.last_allocated_type_id.store(type_id, Relaxed);
                    Ok(type_id)
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
        let next = TypeID::build(self.next_entity.allocate(snapshot)?);
        let vertex = build_vertex_entity_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let next = TypeID::build(self.next_relation.allocate(snapshot)?);
        let vertex = build_vertex_relation_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let next = TypeID::build(self.next_role.allocate(snapshot)?);
        let vertex = build_vertex_role_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(&self, snapshot: &Snapshot) -> Result<TypeVertex<'static>, EncodingError> {
        let next = TypeID::build(self.next_attribute.allocate(snapshot)?);
        let vertex = build_vertex_attribute_type(next);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }
}
