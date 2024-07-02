/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use concept::thing::object::ObjectAPI;
use concept::thing::thing_manager::ThingManager;
use concept::type_::{Ordering, OwnerAPI};
use concept::type_::type_manager::TypeManager;
use durability::wal::WAL;
use encoding::EncodingKeyspace;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::value::label::Label;
use encoding::value::value::Value;
use encoding::value::value_type::ValueType;
use ir::program::block::FunctionalBlock;
use ir::program::program::Program;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, ReadableSnapshot, ReadSnapshot, WriteSnapshot};
use test_utils::{create_tmp_dir, init_logging};

fn setup_storage() -> Arc<MVCCStorage<WALClient>> {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );

    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
        .unwrap();
    storage
}

fn load_managers<Snapshot: ReadableSnapshot>(
    storage: Arc<MVCCStorage<WALClient>>,
) -> (Arc<TypeManager<Snapshot>>, ThingManager<Snapshot>) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage).unwrap());
    let type_manager =
        Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
    let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
    (type_manager, thing_manager)
}

#[test]
fn traverse_has() {
    let storage = setup_storage();

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone());

        let person_label = Label::build("person");
        let age_label = Label::build("age");
        let name_label = Label::build("name");
        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();
        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label, false).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label, false).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();

        let _person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

        let mut _age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
        let mut _age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();
        let mut _age_3 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap();
        let mut _age_4 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap();
        let mut _age_5 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap();

        let mut _name_1 = thing_manager.create_attribute(
            &mut snapshot, name_type.clone(), Value::String(Cow::Owned("John".to_string()))
        ).unwrap();
        let mut _name_2 = thing_manager.create_attribute(
            &mut snapshot, name_type.clone(), Value::String(Cow::Owned("Alice".to_string()))
        ).unwrap();
        let mut _name_3 = thing_manager.create_attribute(
            &mut snapshot, name_type.clone(), Value::String(Cow::Owned("Leila".to_string()))
        ).unwrap();

        _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_1.clone()).unwrap();
        _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_2.clone()).unwrap();
        _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_3.clone()).unwrap();
        _person_1.set_has_unordered(&mut snapshot, &thing_manager, _name_1.clone()).unwrap();
        _person_1.set_has_unordered(&mut snapshot, &thing_manager, _name_2.clone()).unwrap();

        _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_5.clone()).unwrap();
        _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_4.clone()).unwrap();
        _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_1.clone()).unwrap();

        _person_3.set_has_unordered(&mut snapshot, &thing_manager, _age_4.clone()).unwrap();
        _person_3.set_has_unordered(&mut snapshot, &thing_manager, _name_3.clone()).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone());
        let entities = thing_manager.get_entities(&snapshot).collect_cloned();
        assert_eq!(entities.len(), 4);
    }

    // query: match $person has name $name, has age $age; limit 3; filter $person, $age;

    let mut block = FunctionalBlock::new();
    {
        let mut conjunction = block.conjunction_mut();
        let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
        let var_age = conjunction.get_or_declare_variable(&"age").unwrap();
        let var_name = conjunction.get_or_declare_variable(&"name").unwrap();
        conjunction.constraints_mut().add_has(var_person, var_age).unwrap();
        conjunction.constraints_mut().add_has(var_person, var_name).unwrap();
    }
    block.add_limit(3);
    block.add_filter(vec![&"person", &"age"]);
    let mut program = Program::new(block, HashMap::new());

    // let pattern_plan = PatternPlan::new();
    // let program_plan = ProgramPlan::new();
}
