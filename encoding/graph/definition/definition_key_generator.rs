/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */use storage::snapshot::WritableSnapshot;

use crate::{
    error::EncodingError,
    graph::{
        common::schema_id_allocator::DefinitionKeyAllocator,
        definition::definition_key::DefinitionKey,
    },
    Keyable,
};
use crate::layout::prefix::Prefix;

pub struct DefinitionKeyGenerator {
    next_struct: DefinitionKeyAllocator,
}

impl DefinitionKeyGenerator {
    pub fn new() -> DefinitionKeyGenerator {
        Self { next_struct: DefinitionKeyAllocator::new(Prefix::DefinitionStruct) }
    }
    pub fn create_struct<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey<'static>, EncodingError> {
        let definition_key = self.next_struct.allocate(snapshot)?;
        snapshot.put(definition_key.as_storage_key().into_owned_array());
        Ok(definition_key)
    }
}
