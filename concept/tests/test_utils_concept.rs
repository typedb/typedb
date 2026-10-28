/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex, Weak};

use concept::{
    index::vector_index::VectorIndex,
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{TypeManager, type_cache::TypeCache},
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
    type_::vertex_generator::TypeVertexGenerator,
};
use storage::{
    MVCCStorage,
    durability_client::{DurabilityClient, WALClient},
    sequence_number::SequenceNumber,
};

static VECTOR_INDEXES: Mutex<Vec<(Weak<MVCCStorage<WALClient>>, Arc<VectorIndex>)>> = Mutex::new(Vec::new());

pub fn setup_concept_storage(storage: &mut Arc<MVCCStorage<WALClient>>) {
    let vector_index = Arc::new(VectorIndex::new());
    {
        let storage = Arc::get_mut(storage).unwrap();
        storage.durability_mut().register_record_type::<Statistics>();
        storage.set_secondary_store(vector_index.clone());
    }
    let mut stores = VECTOR_INDEXES.lock().unwrap();
    stores.retain(|(weak, _)| weak.strong_count() > 0);
    stores.push((Arc::downgrade(storage), vector_index));
}

pub fn vector_index(storage: &Arc<MVCCStorage<WALClient>>) -> Arc<VectorIndex> {
    VECTOR_INDEXES
        .lock()
        .unwrap()
        .iter()
        .find(|(weak, _)| weak.upgrade().is_some_and(|stored| Arc::ptr_eq(&stored, storage)))
        .map(|(_, store)| store.clone())
        .expect("setup_concept_storage must be called before using the vector store")
}

pub fn load_managers(
    storage: Arc<MVCCStorage<WALClient>>,
    type_cache_at: Option<SequenceNumber>,
) -> (Arc<TypeManager>, Arc<ThingManager>) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise_from_wal(storage.as_ref()).unwrap();
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let vector_index = self::vector_index(&storage);
    let cache = type_cache_at.map(|sequence_number| Arc::new(TypeCache::new(storage, sequence_number).unwrap()));
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, cache));
    let thing_manager = Arc::new(ThingManager::new(
        thing_vertex_generator,
        type_manager.clone(),
        Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
        vector_index,
    ));
    (type_manager, thing_manager)
}
