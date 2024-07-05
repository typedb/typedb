/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use answer::variable_value::VariableValue;
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
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
    inference::type_inference::{ConstraintTypeAnnotations, LeftRightAnnotations, TypeAnnotations},
    program::block::FunctionalBlock,
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};
use traversal::{
    executor::program_executor::ProgramExecutor,
    planner::{
        pattern_plan::{Execution, Iterate, IterateMode, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};

fn setup_storage() -> Arc<MVCCStorage<WALClient>> {
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
    storage
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
    let storage = setup_storage();

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

        let _person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

        let mut _age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
        let mut _age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();
        let mut _age_3 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap();
        let mut _age_4 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap();
        let mut _age_5 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap();

        let mut _name_1 = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("John".to_string())))
            .unwrap();
        let mut _name_2 = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Alice".to_string())))
            .unwrap();
        let mut _name_3 = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Leila".to_string())))
            .unwrap();

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

    // query:
    //   match
    //    $person has name $name, has age $age;
    //   limit 3;
    //   filter $person, $age;

    // IR
    let mut block = FunctionalBlock::new();
    let mut conjunction = block.conjunction_mut();
    let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
    let var_age = conjunction.get_or_declare_variable(&"age").unwrap();
    let var_name = conjunction.get_or_declare_variable(&"name").unwrap();
    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name).unwrap().clone();
    block.add_limit(3);
    let filter = block.add_filter(vec![&"person", &"age"]).unwrap().clone();

    // Plan
    let steps = vec![Step::new(
        Execution::SortedIterators(vec![
            Iterate::Has(has_age.clone(), IterateMode::UnboundSortedFrom),
            Iterate::Has(has_name.clone(), IterateMode::UnboundSortedFrom),
        ]),
        &HashSet::new(),
    )];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone());

        let person_type = type_manager.get_entity_type(&snapshot, &person_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();
        let name_type = type_manager.get_attribute_type(&snapshot, &name_label).unwrap().unwrap();

        // Type Annotations
        let mut variable_annotations = HashMap::new();
        variable_annotations.insert(var_person, Arc::new(HashSet::from([person_type.clone().into()])));
        variable_annotations.insert(var_name, Arc::new(HashSet::from([name_type.clone().into()])));
        variable_annotations.insert(var_age, Arc::new(HashSet::from([age_type.clone().into()])));

        let mut constraint_annotations = HashMap::new();
        constraint_annotations.insert(
            has_age.into(),
            ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(
                BTreeMap::from([(person_type.clone().into(), vec![age_type.clone().into()])]),
                BTreeMap::from([(age_type.clone().into(), vec![person_type.clone().into()])]),
            )),
        );

        constraint_annotations.insert(
            has_name.into(),
            ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(
                BTreeMap::from([(person_type.clone().into(), vec![name_type.clone().into()])]),
                BTreeMap::from([(name_type.clone().into(), vec![person_type.clone().into()])]),
            )),
        );

        let type_annotations = TypeAnnotations::new(variable_annotations, constraint_annotations);
        ProgramExecutor::new(program_plan, &type_annotations, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (type_manager, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<Vec<VariableValue<'static>>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.to_vec()).map_err(|err| err.clone())).collect();

        for row in rows {
            let r = row.unwrap();
            for value in r {
                print!("{}, ", value);
            }
            println!()
        }
    }
}
