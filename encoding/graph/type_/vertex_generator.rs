/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use storage::snapshot::WritableSnapshot;

use crate::{
    error::EncodingError,
    graph::{common::schema_id_allocator::TypeVertexAllocator, type_::vertex::TypeVertex},
    layout::prefix::Prefix,
    Keyable,
};

#[derive(Debug)]
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
    pub fn new() -> TypeVertexGenerator {
        TypeVertexGenerator {
            next_entity: TypeVertexAllocator::new(Prefix::VertexEntityType),
            next_relation: TypeVertexAllocator::new(Prefix::VertexRelationType),
            next_role: TypeVertexAllocator::new(Prefix::VertexRoleType),
            next_attribute: TypeVertexAllocator::new(Prefix::VertexAttributeType),
        }
    }

    pub fn create_entity_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex, EncodingError> {
        let vertex = self.next_entity.allocate(snapshot)?;
        snapshot.put(vertex.into_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_relation_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex, EncodingError> {
        let vertex = self.next_relation.allocate(snapshot)?;
        snapshot.put(vertex.into_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_role_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex, EncodingError> {
        let vertex = self.next_role.allocate(snapshot)?;
        snapshot.put(vertex.into_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn create_attribute_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<TypeVertex, EncodingError> {
        let vertex = self.next_attribute.allocate(snapshot)?;
        snapshot.put(vertex.into_storage_key().into_owned_array());
        Ok(vertex)
    }

    pub fn reset(&mut self) {
        self.next_entity.reset();
        self.next_relation.reset();
        self.next_attribute.reset();
        self.next_role.reset();
    }
}
