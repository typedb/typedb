/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compiler::{
    self,
    annotation::{
        function::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        match_inference::infer_types,
    },
    executable::match_::{
        instructions::{
            thing::{IsaInstruction, IsaReverseInstruction},
            ConstraintInstruction, Inputs,
        },
        planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
        },
    },
    ExecutorVariable, VariablePosition,
};
use encoding::value::label::Label;
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use ir::{
    pattern::{constraint::IsaKind, Vertex},
    pipeline::block::Block,
    translation::TranslationContext,
};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const ANIMAL_LABEL: Label = Label::new_static("animal");
const CAT_LABEL: Label = Label::new_static("cat");
const DOG_LABEL: Label = Label::new_static("dog");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

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
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into()).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_dog, var_dog_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_unbounded_sorted_type() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into()).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_dog, var_dog_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog_type],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_bounded_thing() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa! $t; $x isa $u;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_type_from = conjunction.get_or_declare_variable("t").unwrap();
    let var_type_to = conjunction.get_or_declare_variable("u").unwrap();
    let var_thing = conjunction.get_or_declare_variable("x").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_type =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing, var_type_from.into()).unwrap().clone();
    let isa_to_type =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_to.into()).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_thing, var_type_to];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_type_from, var_thing, var_type_to].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_type_from],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_from_type, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_thing]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_type_to],
            vec![ConstraintInstruction::Isa(
                IsaInstruction::new(isa_to_type, Inputs::Single([var_thing]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_type_to]],
            &named_variables,
            2,
        )),
    ];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    // 2x animal x {animal} = 2
    // 3x dog x {animal, dog} = 6
    // 1x cat x {animal, cat} = 2
    assert_eq!(rows.len(), 10);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_unbounded_sorted_thing() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into()).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_dog, var_dog_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog],
        vec![ConstraintInstruction::IsaReverse(
            IsaReverseInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_unbounded_sorted_type() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into()).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_dog, var_dog_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_dog, var_dog_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog_type],
        vec![ConstraintInstruction::IsaReverse(
            IsaReverseInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_bounded_type_exact() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa! $t; $y isa! $t;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_thing_from = conjunction.get_or_declare_variable("x").unwrap();
    let var_thing_to = conjunction.get_or_declare_variable("y").unwrap();
    let var_type = conjunction.get_or_declare_variable("t").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing_from, var_type.into()).unwrap().clone();
    let isa_to_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing_to, var_type.into()).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_thing_from, var_type, var_thing_to];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_thing_from, var_type, var_thing_to].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_thing_from],
            vec![ConstraintInstruction::Isa(
                IsaInstruction::new(isa_from_thing, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_thing_from], variable_positions[&var_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_thing_to],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_to_thing, Inputs::Single([var_type]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_thing_from], variable_positions[&var_type], variable_positions[&var_thing_to]],
            &named_variables,
            3,
        )),
    ];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    // 2x animal => 4x (animal x animal)
    // 3x dog => 9x (dog x dog)
    // 1x cat => 1x (cat x cat)
    // 4 + 9 + 1 = 14
    assert_eq!(rows.len(), 14);
    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}

#[test]
fn traverse_isa_reverse_bounded_type_subtype() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $y isa $t;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_thing_from = conjunction.get_or_declare_variable("x").unwrap();
    let var_thing_to = conjunction.get_or_declare_variable("y").unwrap();
    let var_type = conjunction.get_or_declare_variable("t").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_from, var_type.into()).unwrap().clone();
    let isa_to_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_to, var_type.into()).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_thing_from, var_type, var_thing_to];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_thing_from, var_type, var_thing_to].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_thing_from],
            vec![ConstraintInstruction::Isa(
                IsaInstruction::new(isa_from_thing, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_thing_from], variable_positions[&var_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_thing_to],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_to_thing, Inputs::Single([var_type]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_thing_from], variable_positions[&var_type], variable_positions[&var_thing_to]],
            &named_variables,
            3,
        )),
    ];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }

    // 6x animal => 36x (animal x animal)
    // 3x dog => 9x (dog x dog)
    // 1x cat => 1x (cat x cat)
    // 36 + 9 + 1 = 46
    assert_eq!(rows.len(), 46);
}

#[test]
fn traverse_isa_reverse_fixed_type_exact() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa! animal;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_thing = conjunction.get_or_declare_variable("x").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing, Vertex::Label(ANIMAL_LABEL)).unwrap().clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_thing];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_thing].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_thing],
        vec![ConstraintInstruction::IsaReverse(
            IsaReverseInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_thing]],
        &named_variables,
        1,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }

    assert_eq!(rows.len(), 2);
}

#[test]
fn traverse_isa_reverse_fixed_type_subtype() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa animal;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_thing = conjunction.get_or_declare_variable("x").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_thing, Vertex::Label(ANIMAL_LABEL))
        .unwrap()
        .clone();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_thing];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_thing].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_thing],
        vec![ConstraintInstruction::IsaReverse(
            IsaReverseInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_thing]],
        &named_variables,
        1,
    ))];

    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, ReadExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }

    assert_eq!(rows.len(), 6);
}
