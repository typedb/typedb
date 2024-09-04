/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::sync::Arc;

use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use durability::DurabilitySequenceNumber;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use storage::durability_client::{DurabilityClient, WALClient};
use storage::MVCCStorage;

pub fn setup_concept_storage(storage: &mut Arc<MVCCStorage<WALClient>>) {
    let mut storage = Arc::get_mut(storage).unwrap();
    storage.durability_mut().register_record_type::<Statistics>();
}

pub fn load_managers(storage: Arc<MVCCStorage<WALClient>>) -> (Arc<TypeManager>, Arc<ThingManager>) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise(storage.as_ref()).unwrap();
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage).unwrap());
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, None));
    let thing_manager = Arc::new(ThingManager::new(
        thing_vertex_generator,
        type_manager.clone(),
        Arc::new(statistics),
    ));
    (type_manager, thing_manager)
}
