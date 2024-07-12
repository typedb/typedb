/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use concept::{
    thing::{object::ObjectAPI, statistics::Statistics, thing_manager::ThingManager},
    type_::{type_manager::TypeManager, Ordering, OwnerAPI},
};
use durability::wal::WAL;
use encoding::{
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    value::{label::Label, value::Value, value_type::ValueType},
    EncodingKeyspace,
};
use ir::{
    inference::type_inference::infer_types,
    pattern::constraint::Has,
    program::{block::FunctionalBlock, program::Program},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    sequence_number::SequenceNumber,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging, TempDir};
use traversal::{
    executor::program_executor::ProgramExecutor,
    planner::{
        pattern_plan::{Execution, Iterate, IterateMode, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};

fn setup_storage() -> (Arc<MVCCStorage<WALClient>>, TempDir) {
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
    (storage, storage_path)
}

fn load_managers(storage: Arc<MVCCStorage<WALClient>>) -> (Arc<TypeManager>, ThingManager) {
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
    let (storage, _storage_dir_guard) = setup_storage();

    let person_label = Label::build("person");
    let age_label = Label::build("age");
    let name_label = Label::build("name");

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();

    {
        let (type_manager, thing_manager) = load_managers(storage.clone());

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();
        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label, false).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label, false).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();

        let person = [
            thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
            thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
            thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        ];

        let age = [
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap(),
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap(),
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap(),
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap(),
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap(),
        ];

        let name = [
            thing_manager
                .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("John".to_string())))
                .unwrap(),
            thing_manager
                .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Alice".to_string())))
                .unwrap(),
            thing_manager
                .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Leila".to_string())))
                .unwrap(),
        ];

        person[0].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
        person[0].set_has_unordered(&mut snapshot, &thing_manager, age[1].clone()).unwrap();
        person[0].set_has_unordered(&mut snapshot, &thing_manager, age[2].clone()).unwrap();
        person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
        person[0].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();

        person[1].set_has_unordered(&mut snapshot, &thing_manager, age[4].clone()).unwrap();
        person[1].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
        person[1].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();

        person[2].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
        person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    let query = "match $person isa person, has name $name, has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let mut builder = FunctionalBlock::builder();
    builder.conjunction_mut().and_typeql_patterns(&match_.patterns).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let program = Program::new(block, HashMap::new());
    let type_annotations = infer_types(&program, &snapshot, &type_manager);
    let pattern_plan = PatternPlan::from_block(program.entry, &type_annotations, &statistics);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());
    let executor = ProgramExecutor::new(program_plan, &type_annotations, &snapshot, &thing_manager).unwrap();

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows = iterator
            .map_static(|row| row.map(|row| row.to_vec()).map_err(|err| err.clone()))
            .into_iter()
            .try_collect::<_, Vec<_>, _>()
            .unwrap();

        assert_eq!(rows.len(), 7);

        for row in rows {
            for value in row {
                print!("{}, ", value);
            }
            println!()
        }
    }
}
