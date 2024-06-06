/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use encoding::graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinition, DefinitionValueEncoding};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

pub struct StructDefinitionCache {
    pub(super) definition_key: DefinitionKey<'static>,
    pub(super) definition: StructDefinition,
}

impl StructDefinitionCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<StructDefinitionCache>]> {
        let definitions = snapshot
            .iterate_range(KeyRange::new_within(
                DefinitionKey::build_prefix(StructDefinition::PREFIX),
                StructDefinition::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_hashmap(|key, value| {
                (DefinitionKey::new(Bytes::Array(key.byte_ref().into())), StructDefinition::from_bytes(value))
            })
            .unwrap();

        let max_definition_id = definitions.iter().map(|(d, _)| d.definition_id().as_uint()).max().unwrap_or(0);
        let mut caches = (0..=max_definition_id).map(|_| None).collect::<Box<[_]>>();
        for (key, definition) in definitions.into_iter() {
            let cache = StructDefinitionCache { definition_key: key.clone(), definition };
            caches[key.definition_id().as_uint() as usize] = Some(cache);
        }

        caches
    }
}
