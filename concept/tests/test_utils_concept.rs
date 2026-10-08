/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex, Weak};

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager, vector_store::VectorStore},
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

// one VectorStore per storage, shared across repeated load_managers calls so commits and reads
// see the same store. Keyed by storage identity because the commit-observer trait object cannot
// be downcast back to a VectorStore.
static VECTOR_STORES: Mutex<Vec<(Weak<MVCCStorage<WALClient>>, Arc<VectorStore>)>> = Mutex::new(Vec::new());

pub fn setup_concept_storage(storage: &mut Arc<MVCCStorage<WALClient>>) {
    let vector_store = Arc::new(VectorStore::new());
    {
        let storage = Arc::get_mut(storage).unwrap();
        storage.durability_mut().register_record_type::<Statistics>();
        storage.set_commit_observer(vector_store.clone());
    }
    let mut stores = VECTOR_STORES.lock().unwrap();
    stores.retain(|(weak, _)| weak.strong_count() > 0);
    stores.push((Arc::downgrade(storage), vector_store));
}

pub fn vector_store(storage: &Arc<MVCCStorage<WALClient>>) -> Arc<VectorStore> {
    VECTOR_STORES
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
    statistics.may_synchronise(storage.as_ref()).unwrap();
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let vector_store = self::vector_store(&storage);
    let cache = type_cache_at.map(|sequence_number| Arc::new(TypeCache::new(storage, sequence_number).unwrap()));
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, cache));
    let thing_manager = Arc::new(ThingManager::new(
        thing_vertex_generator,
        type_manager.clone(),
        Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
        vector_store,
    ));
    (type_manager, thing_manager)
}
