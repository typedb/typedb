/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::Arc,
};
use typeql::builder::type_;

use answer::variable_value::VariableValue;
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{Ordering, OwnerAPI, type_manager::TypeManager},
};
use durability::wal::WAL;
use encoding::{
    EncodingKeyspace,
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator,
        thing::vertex_generator::ThingVertexGenerator,
        type_::{Kind, vertex_generator::TypeVertexGenerator},
    },
    value::{label::Label, value::Value, value_type::ValueType},
};
use ir::{
    inference::type_inference::infer_types,
    pattern::constraint::IsaKind,
    program::{block::FunctionalBlock, program::Program},
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    MVCCStorage,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
};
use test_utils::{create_tmp_dir, init_logging, TempDir};
use traversal::{
    executor::program_executor::ProgramExecutor,
    planner::{
        pattern_plan::{Instruction, PatternPlan, SortedJoinStep, Step},
        program_plan::ProgramPlan,
    },
};
use traversal::planner::pattern_plan::IterateBounds;
use crate::common::{load_managers, setup_storage};

const ANIMAL_LABEL: Label = Label::new_static("animal");
const CAT_LABEL: Label = Label::new_static("cat");
const DOG_LABEL: Label = Label::new_static("dog");

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let animal_type = type_manager.create_entity_type(&mut snapshot, &ANIMAL_LABEL, false).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL, false).unwrap();
    let cat_type = type_manager.create_entity_type(&mut snapshot, &CAT_LABEL, false).unwrap();
    dog_type.set_supertype(&mut snapshot, &type_manager, animal_type.clone()).unwrap();
    cat_type.set_supertype(&mut snapshot, &type_manager, animal_type.clone()).unwrap();

    let _animal_1 = thing_manager.create_entity(&mut snapshot, animal_type.clone()).unwrap();
    let _animal_2 = thing_manager.create_entity(&mut snapshot, animal_type.clone()).unwrap();

    let _dog_1 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();
    let _dog_2 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();
    let _dog_3 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();

    let _cat_1 = thing_manager.create_entity(&mut snapshot, cat_type.clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn traverse_isa_unbounded_sorted_thing() {
    let (tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable_named(&"dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable_named(&"dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let filter = block.add_filter(vec!["dog"]).unwrap().clone();
    let program = Program::new(block.finish(), HashMap::new());

    let type_annotations = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(&program, &snapshot, &type_manager).unwrap()
    };

    // Plan
    let steps = vec![Step::SortedJoin(SortedJoinStep::new(
        var_dog,
        vec![
            Instruction::Isa(isa.clone(), IterateBounds::None([])),
        ],
        &vec![var_dog, var_dog_type]
    ))];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps, program.entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, &type_annotations, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<Vec<VariableValue<'static>>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.to_vec()).map_err(|err| err.clone())).collect();
        assert_eq!(rows.len(), 3);

        for row in rows {
            let r = row.unwrap();
            for value in r {
                print!("{}, ", value);
            }
            println!()
        }
    }
}

#[test]
fn traverse_has_unbounded_sorted_to_merged() {
    let (tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person has attribute $attribute;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable_named(&"person_type").unwrap();
    let var_attribute_type = conjunction.get_or_declare_variable_named(&"attr_type").unwrap();
    let var_person = conjunction.get_or_declare_variable_named(&"person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable_named(&"attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, ANIMAL_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_attribute_type, Kind::Attribute.root_label().scoped_name().as_str())
        .unwrap();
    let program = Program::new(block.finish(), HashMap::new());

    let type_annotations = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(&program, &snapshot, &type_manager).unwrap()
    };

    // Plan
    let steps = vec![Step::SortedJoin(SortedJoinStep::new(
        var_attribute,
        vec![Instruction::Has(has_attribute.clone(), IterateBounds::None([]))],
        &vec![var_person, var_attribute],
    ))];
    let pattern_plan = PatternPlan::new(steps, program.entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, &type_annotations, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let variable_positions = executor.entry_variable_positions().clone();

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<Vec<VariableValue<'static>>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.to_vec()).map_err(|err| err.clone())).collect();

        // person 1 - has age 1, has age 2, has age 3, has name 1, has name 2 => 5 answers
        // person 2 - has age 1, has age 4, has age 5 => 3 answers
        // person 3 - has age 4, has name 3 => 2 answers

        for row in &rows {
            let r = row.as_ref().unwrap();
            for value in r {
                print!("{}, ", value);
            }
            println!()
        }
        assert_eq!(rows.len(), 10);

        let attribute_position = variable_positions.get(&var_attribute).unwrap().as_usize();
        let mut last_attribute = &rows[0].as_ref().unwrap()[attribute_position];
        for row in &rows {
            let r = row.as_ref().unwrap();
            let attribute = &r[attribute_position];
            assert!(last_attribute <= attribute, "{} <= {} failed", &last_attribute, &attribute);
            last_attribute = attribute;
        }
    }
}
