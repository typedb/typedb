/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use storage::snapshot::WritableSnapshot;

use crate::{
    Keyable,
    error::EncodingError,
    graph::{
        common::schema_id_allocator::{DefinitionKeyAllocator, RBACVertexAllocator},
        definition::definition_key::DefinitionKey,
        rbac::RBACRoleVertex,
    },
    layout::prefix::Prefix,
};

#[derive(Debug)]
pub struct DefinitionKeyGenerator {
    next_struct: DefinitionKeyAllocator,
    next_function: DefinitionKeyAllocator,
    next_rbac_role: RBACVertexAllocator,
}

impl Default for DefinitionKeyGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl DefinitionKeyGenerator {
    pub fn new() -> DefinitionKeyGenerator {
        Self {
            next_struct: DefinitionKeyAllocator::new(Prefix::DefinitionStruct),
            next_function: DefinitionKeyAllocator::new(Prefix::DefinitionFunction),
            next_rbac_role: RBACVertexAllocator::new(),
        }
    }

    pub fn reset(&mut self) {
        self.next_struct.reset();
        self.next_function.reset();
        self.next_rbac_role.reset();
    }

    pub fn create_struct<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey, EncodingError> {
        let definition_key = self.next_struct.allocate(snapshot)?;
        snapshot.put(definition_key.clone().into_storage_key().into_owned_array());
        Ok(definition_key)
    }

    pub fn create_function<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey, EncodingError> {
        let definition_key = self.next_function.allocate(snapshot)?;
        snapshot.put(definition_key.clone().into_storage_key().into_owned_array());
        Ok(definition_key)
    }

    pub fn create_rbac_role<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<RBACRoleVertex, EncodingError> {
        let rbac_role_vertex = self.next_rbac_role.allocate(snapshot)?;
        snapshot.put(rbac_role_vertex.clone().into_storage_key().into_owned_array());
        Ok(rbac_role_vertex)
    }
}
