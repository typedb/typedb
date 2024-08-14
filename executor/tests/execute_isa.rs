/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::{
    inference::{
        annotated_functions::{AnnotatedCommittedFunctions, IndexedAnnotatedFunctions},
        type_inference::infer_types,
    },
    instruction::constraint::instructions::{ConstraintInstruction, Inputs, IsaInstruction, IsaReverseInstruction},
    planner::{
        pattern_plan::{IntersectionStep, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};
use concept::error::ConceptReadError;
use encoding::value::label::Label;
use executor::{batch::ImmutableRow, program_executor::ProgramExecutor};
use ir::{
    pattern::constraint::IsaKind,
    program::{block::FunctionalBlock, program::Program},
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
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
fn traverse_isa_unbounded_from_thing() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $x isa $u;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_type_from = conjunction.get_or_declare_variable("t").unwrap();
    let var_type_to = conjunction.get_or_declare_variable("u").unwrap();
    let var_thing = conjunction.get_or_declare_variable("x").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_type =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_from).unwrap().clone();
    let isa_to_type = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_to).unwrap().clone();
    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![
        Step::Intersection(IntersectionStep::new(
            var_thing,
            vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                isa_from_type,
                Inputs::None([]),
                annotated_program.entry_annotations(),
            ))],
            &[var_thing],
        )),
        Step::Intersection(IntersectionStep::new(
            var_thing,
            vec![ConstraintInstruction::Isa(IsaInstruction::new(
                isa_to_type,
                Inputs::Single([var_thing]),
                annotated_program.entry_annotations(),
            ))],
            &[var_type_to],
        )),
    ];

    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    // Executor
    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 6);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
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
    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(IndexedAnnotatedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_dog,
        vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
            isa,
            Inputs::None([]),
            annotated_program.entry_annotations(),
        ))],
        &[var_dog, var_dog_type],
    ))];

    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());

    // Executor
    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}
