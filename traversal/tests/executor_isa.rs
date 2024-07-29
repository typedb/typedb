/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use concept::{error::ConceptReadError, thing::object::ObjectAPI, type_::OwnerAPI};
use encoding::value::label::Label;
use ir::{
    inference::type_inference::infer_types,
    pattern::constraint::IsaKind,
    program::{
        block::FunctionalBlock,
        program::{CompiledSchemaFunctions, Program},
    },
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};
use traversal::{
    executor::{batch::ImmutableRow, program_executor::ProgramExecutor},
    planner::{
        pattern_plan::{Instruction, IntersectionStep, IterateBounds, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};

use crate::common::{load_managers, setup_storage};

mod common;

const ANIMAL_LABEL: Label = Label::new_static("animal");
const CAT_LABEL: Label = Label::new_static("cat");
const DOG_LABEL: Label = Label::new_static("dog");

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let animal_type = type_manager.create_entity_type(&mut snapshot, &ANIMAL_LABEL).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL).unwrap();
    let cat_type = type_manager.create_entity_type(&mut snapshot, &CAT_LABEL).unwrap();
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
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let filter = block.add_filter(vec!["dog"]).unwrap().clone();
    let program = Program::new(block.finish(), Vec::new());
    let schema_cache = CompiledSchemaFunctions::new(Box::new([]), Box::new([]));

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_dog,
        vec![Instruction::Isa(isa.clone(), IterateBounds::None([]))],
        &vec![var_dog, var_dog_type],
    ))];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan =
        ProgramPlan::new(pattern_plan, annotated_program.get_entry_annotations().clone(), HashMap::new());

    let annotated_program = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(program, &snapshot, &type_manager, Arc::new(schema_cache)).unwrap()
    };

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, annotated_program.get_entry_annotations(), &snapshot, &thing_manager)
            .unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
        assert_eq!(rows.len(), 3);

        for row in rows {
            let row = row.unwrap();
            assert_eq!(row.get_multiplicity(), 1);
            print!("{}", row);
        }
    }
}

#[test]
fn traverse_has_unbounded_sorted_to_merged() {
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person has $attribute;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_attribute_type = conjunction.get_or_declare_variable("attr_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable("attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, ANIMAL_LABEL.scoped_name().as_str()).unwrap();
    let program = Program::new(block.finish(), Vec::new());
    let schema_cache = CompiledSchemaFunctions::new(Box::new([]), Box::new([]));

    // Plan
    let steps = vec![Step::SortedJoin(SortedJoinStep::new(
        var_attribute,
        vec![Instruction::Has(has_attribute.clone(), IterateBounds::None([]))],
        &vec![var_person, var_attribute],
    ))];
    let pattern_plan = PatternPlan::new(steps, program.entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    let annotated_program = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(program, &snapshot, &type_manager, Arc::new(schema_cache)).unwrap()
    };

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, annotated_program.get_entry_annotations(), &snapshot, &thing_manager)
            .unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let variable_positions = executor.entry_variable_positions().clone();

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

        // person 1 - has age 1, has age 2, has age 3, has name 1, has name 2 => 5 answers
        // person 2 - has age 1, has age 4, has age 5 => 3 answers
        // person 3 - has age 4, has name 3 => 2 answers

        assert_eq!(rows.len(), 10);

        for row in &rows {
            let r = row.as_ref().unwrap();
            debug_assert_eq!(r.get_multiplicity(), 1);
            print!("{}", r);
        }

        let attribute_position = variable_positions[&var_attribute];
        let mut last_attribute = rows[0].as_ref().unwrap().get(attribute_position);
        for row in &rows {
            let row = row.as_ref().unwrap();
            let attribute = &row.get(attribute_position);
            assert!(last_attribute <= attribute, "{} <= {} failed", &last_attribute, &attribute);
            last_attribute = attribute;
        }
    }
}
