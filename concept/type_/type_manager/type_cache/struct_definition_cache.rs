/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinition};
use storage::snapshot::ReadableSnapshot;

use crate::type_::type_manager::type_reader::TypeReader;

#[derive(Debug)]
pub struct StructDefinitionCache {
    pub(super) definition_key: DefinitionKey,
    pub(super) definition: StructDefinition,
}

impl StructDefinitionCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<StructDefinitionCache>]> {
        let definitions = TypeReader::get_struct_definitions_all(snapshot).unwrap();

        let max_definition_id = definitions.keys().map(|d| d.definition_id().as_uint()).max().unwrap_or(0);
        let mut caches = (0..=max_definition_id).map(|_| None).collect::<Box<[_]>>();
        for (key, definition) in definitions.into_iter() {
            let cache = StructDefinitionCache { definition_key: key.clone(), definition };
            caches[key.definition_id().as_uint() as usize] = Some(cache);
        }

        caches
    }
}
