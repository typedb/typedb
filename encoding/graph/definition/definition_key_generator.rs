/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::encoding::DefinitionIDAtomicUInt;
use storage::snapshot::WritableSnapshot;
use crate::error::EncodingError;
use crate::graph::definition::definition_key::{DefinitionID, DefinitionKey};
use crate::Keyable;
use crate::layout::prefix::Prefix;

pub struct DefinitionKeyGenerator {
    // TODO: implement full allocator with recycling
    next_struct_id: DefinitionIDAtomicUInt,
}

impl DefinitionKeyGenerator {

    pub fn create_struct<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<DefinitionKey<'static>, EncodingError> {

        // TODO: implement
        let id = 0;

        let definition_key = DefinitionKey::build(Prefix::DefinitionStruct, DefinitionID::build(id));
        snapshot.put(definition_key.as_storage_key().into_owned_array());
        Ok(definition_key)
    }
}


