/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::{
    match_::{
        inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
        instructions::{ConstraintInstruction, Inputs, IsaInstruction, IsaReverseInstruction},
        planner::{
            pattern_plan::{IntersectionProgram, MatchProgram, Program},
            program_plan::ProgramPlan,
        },
    },
    VariablePosition,
};
use concept::error::ConceptReadError;
use encoding::value::label::Label;
use executor::{program_executor::ProgramExecutor, row::MaybeOwnedRow};
use ir::{pattern::constraint::IsaKind, program::block::FunctionalBlock, translation::TranslationContext};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};

use crate::common::{load_managers, setup_storage};

mod common;

const ANIMAL_LABEL: Label = Label::new_static("animal");
const CAT_LABEL: Label = Label::new_static("cat");
const DOG_LABEL: Label = Label::new_static("dog");

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let animal_type = type_manager.create_entity_type(&mut snapshot, &ANIMAL_LABEL).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL).unwrap();
    let cat_type = type_manager.create_entity_type(&mut snapshot, &CAT_LABEL).unwrap();
    dog_type.set_supertype(&mut snapshot, &type_manager, &thing_manager, animal_type.clone()).unwrap();
    cat_type.set_supertype(&mut snapshot, &type_manager, &thing_manager, animal_type.clone()).unwrap();

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
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_dog],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(
            isa.map(&variable_positions),
            Inputs::None([]),
            &entry_annotations,
        ))],
        &[variable_positions[&var_dog], variable_positions[&var_dog_type]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_unbounded_sorted_type() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_dog_type],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(
            isa.map(&variable_positions),
            Inputs::None([]),
            &entry_annotations,
        ))],
        &[variable_positions[&var_dog], variable_positions[&var_dog_type]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_bounded_thing() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $x isa $u;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_type_from = conjunction.get_or_declare_variable("t").unwrap();
    let var_type_to = conjunction.get_or_declare_variable("u").unwrap();
    let var_thing = conjunction.get_or_declare_variable("x").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_type =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_from).unwrap().clone();
    let isa_to_type = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_to).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_type_from, var_thing, var_type_to];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![
        Program::Intersection(IntersectionProgram::new(
            variable_positions[&var_type_from],
            vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                isa_from_type.map(&variable_positions),
                Inputs::None([]),
                &entry_annotations,
            ))],
            &[variable_positions[&var_thing]],
        )),
        Program::Intersection(IntersectionProgram::new(
            variable_positions[&var_type_to],
            vec![ConstraintInstruction::Isa(IsaInstruction::new(
                isa_to_type.map(&variable_positions),
                Inputs::Single([variable_positions[&var_thing]]),
                &entry_annotations,
            ))],
            &[variable_positions[&var_type_to]],
        )),
    ];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 6);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_unbounded_sorted_thing() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_dog],
        vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
            isa.map(&variable_positions),
            Inputs::None([]),
            &entry_annotations,
        ))],
        &[variable_positions[&var_dog], variable_positions[&var_dog_type]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_unbounded_sorted_type() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_dog_type],
        vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
            isa.map(&variable_positions),
            Inputs::None([]),
            &entry_annotations,
        ))],
        &[variable_positions[&var_dog], variable_positions[&var_dog_type]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_bounded_type() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $y isa $t;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_thing_from = conjunction.get_or_declare_variable("x").unwrap();
    let var_thing_to = conjunction.get_or_declare_variable("y").unwrap();
    let var_type = conjunction.get_or_declare_variable("t").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_from, var_type).unwrap().clone();
    let isa_to_thing = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_to, var_type).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let vars = vec![var_thing_from, var_type, var_thing_to];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let entry_annotations = entry_annotations.map(&variable_positions);

    // Plan
    let steps = vec![
        Program::Intersection(IntersectionProgram::new(
            variable_positions[&var_thing_from],
            vec![ConstraintInstruction::Isa(IsaInstruction::new(
                isa_from_thing.map(&variable_positions),
                Inputs::None([]),
                &entry_annotations,
            ))],
            &[variable_positions[&var_thing_from], variable_positions[&var_type]],
        )),
        Program::Intersection(IntersectionProgram::new(
            variable_positions[&var_thing_to],
            vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                isa_to_thing.map(&variable_positions),
                Inputs::Single([variable_positions[&var_type]]),
                &entry_annotations,
            ))],
            &[variable_positions[&var_thing_from], variable_positions[&var_type], variable_positions[&var_thing_to]],
        )),
    ];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    // 2x animal => 4x (animal x animal)
    // 3x dog => 9x (dog x dog)
    // 1x cat => 1x (cat x cat)
    // 4 + 9 + 1 = 14
    assert_eq!(rows.len(), 14);
    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}
