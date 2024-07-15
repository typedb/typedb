/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use durability::wal::WAL;
use encoding::EncodingKeyspace;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use test_utils::{create_tmp_dir, init_logging, TempDir};

pub fn setup_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );

    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::initialise_types(storage.clone(), definition_key_generator.clone(), type_vertex_generator.clone())
        .unwrap();
    (storage_path, storage)
}

pub fn load_managers(storage: Arc<MVCCStorage<WALClient>>) -> (Arc<TypeManager>, ThingManager) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage).unwrap());
    let type_manager =
        Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
    let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
    (type_manager, thing_manager)
}