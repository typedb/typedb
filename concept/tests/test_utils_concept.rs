/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{type_cache::TypeCache, TypeManager},
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
    type_::vertex_generator::TypeVertexGenerator,
};
use storage::{
    durability_client::{DurabilityClient, WALClient},
    MVCCStorage,
    sequence_number::SequenceNumber,
};

pub fn setup_concept_storage(storage: &mut Arc<MVCCStorage<WALClient>>) {
    let storage = Arc::get_mut(storage).unwrap();
    storage.durability_mut().register_record_type::<Statistics>();
}

pub fn load_managers(
    storage: Arc<MVCCStorage<WALClient>>,
    type_cache_at: Option<SequenceNumber>,
) -> (Arc<TypeManager>, Arc<ThingManager>) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise(storage.as_ref()).unwrap();
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let cache = type_cache_at.map(|sequence_number| Arc::new(TypeCache::new(storage, sequence_number).unwrap()));
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, cache));
    let thing_manager = Arc::new(ThingManager::new(
        thing_vertex_generator,
        type_manager.clone(),
        Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
    ));
    (type_manager, thing_manager)
}
