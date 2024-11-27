/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use storage::snapshot::WritableSnapshot;

use crate::{
    error::EncodingError,
    graph::{common::schema_id_allocator::DefinitionKeyAllocator, definition::definition_key::DefinitionKey},
    layout::prefix::Prefix,
    Keyable,
};

#[derive(Debug)]
pub struct DefinitionKeyGenerator {
    next_struct: DefinitionKeyAllocator,
    next_function: DefinitionKeyAllocator,
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
        }
    }

    pub fn create_struct<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey<'static>, EncodingError> {
        let definition_key = self.next_struct.allocate(snapshot)?;
        snapshot.put(definition_key.clone().into_storage_key().into_owned_array());
        Ok(definition_key)
    }

    pub fn reset(&mut self) {
        self.next_struct.reset()
    }

    pub fn create_function<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey<'static>, EncodingError> {
        let definition_key = self.next_function.allocate(snapshot)?;
        snapshot.put(definition_key.clone().into_storage_key().into_owned_array());
        Ok(definition_key)
    }
}
