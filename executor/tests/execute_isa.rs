/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use compiler::{
    self,
    annotation::{function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types},
    executable::{
        function::ExecutableFunctionRegistry,
        match_::{
            instructions::{
                thing::{IsaInstruction, IsaReverseInstruction},
                ConstraintInstruction, Inputs,
            },
            planner::{
                match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
                plan::PlannerStatistics,
            },
        },
        next_executable_id,
    },
    ExecutorVariable, VariablePosition,
};
use encoding::value::label::Label;
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, profile::QueryProfile,
    row::MaybeOwnedRow, ExecutionInterrupt,
};
use ir::{
    pattern::{constraint::IsaKind, Vertex},
    pipeline::{block::Block, ParameterRegistry},
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
    dog_type.set_supertype(&mut snapshot, &type_manager, &thing_manager, animal_type).unwrap();
    cat_type.set_supertype(&mut snapshot, &type_manager, &thing_manager, animal_type).unwrap();

    let _animal_1 = thing_manager.create_entity(&mut snapshot, animal_type).unwrap();
    let _animal_2 = thing_manager.create_entity(&mut snapshot, animal_type).unwrap();

    let _dog_1 = thing_manager.create_entity(&mut snapshot, dog_type).unwrap();
    let _dog_2 = thing_manager.create_entity(&mut snapshot, dog_type).unwrap();
    let _dog_3 = thing_manager.create_entity(&mut snapshot, dog_type).unwrap();

    let _cat_1 = thing_manager.create_entity(&mut snapshot, cat_type).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

fn position_mapping<const N: usize, const M: usize>(
    row_vars: [Variable; N],
    internal_vars: [Variable; M],
) -> (
    HashMap<ExecutorVariable, Variable>,
    HashMap<Variable, VariablePosition>,
    HashMap<Variable, ExecutorVariable>,
    HashSet<ExecutorVariable>,
) {
    let position_to_var: HashMap<_, _> =
        row_vars.into_iter().enumerate().map(|(i, v)| (ExecutorVariable::new_position(i as _), v)).collect();
    let variable_positions =
        HashMap::from_iter(position_to_var.iter().map(|(i, var)| (*var, i.as_position().unwrap())));
    let mapping: HashMap<_, _> = row_vars
        .into_iter()
        .map(|var| (var, ExecutorVariable::RowPosition(variable_positions[&var])))
        .chain(internal_vars.into_iter().map(|var| (var, ExecutorVariable::Internal(var))))
        .collect();
    let named_variables = mapping.values().copied().collect();
    (position_to_var, variable_positions, mapping, named_variables)
}

#[test]
fn traverse_isa_unbounded_sorted_thing() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.constraints_mut().get_or_declare_variable("dog_type", None).unwrap();
    let var_dog = conjunction.constraints_mut().get_or_declare_variable("dog", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_dog, var_dog_type], []);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();
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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.constraints_mut().get_or_declare_variable("dog_type", None).unwrap();
    let var_dog = conjunction.constraints_mut().get_or_declare_variable("dog", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_dog, var_dog_type], []);

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

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();
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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_type_from = conjunction.constraints_mut().get_or_declare_variable("t", None).unwrap();
    let var_type_to = conjunction.constraints_mut().get_or_declare_variable("u", None).unwrap();
    let var_thing = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_type =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing, var_type_from.into(), None).unwrap().clone();
    let isa_to_type =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing, var_type_to.into(), None).unwrap().clone();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_thing, var_type_to], [var_type_from]);

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

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();

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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.constraints_mut().get_or_declare_variable("dog_type", None).unwrap();
    let var_dog = conjunction.constraints_mut().get_or_declare_variable("dog", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_dog, var_dog_type], []);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_dog],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_dog], variable_positions[&var_dog_type]],
        &named_variables,
        2,
    ))];

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();
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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.constraints_mut().get_or_declare_variable("dog_type", None).unwrap();
    let var_dog = conjunction.constraints_mut().get_or_declare_variable("dog", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_dog, var_dog_type], []);

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

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();
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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_thing_from = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();
    let var_thing_to = conjunction.constraints_mut().get_or_declare_variable("y", None).unwrap();
    let var_type = conjunction.constraints_mut().get_or_declare_variable("t", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing_from, var_type.into(), None).unwrap().clone();
    let isa_to_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing_to, var_type.into(), None).unwrap().clone();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_thing_from, var_type, var_thing_to], []);

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

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();

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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_thing_from = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();
    let var_thing_to = conjunction.constraints_mut().get_or_declare_variable("y", None).unwrap();
    let var_type = conjunction.constraints_mut().get_or_declare_variable("t", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa_from_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_from, var_type.into(), None).unwrap().clone();
    let isa_to_thing =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_thing_to, var_type.into(), None).unwrap().clone();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_thing_from, var_type, var_thing_to], []);

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

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();

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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_thing = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa =
        conjunction.constraints_mut().add_isa(IsaKind::Exact, var_thing, Vertex::Label(ANIMAL_LABEL), None) .unwrap().clone();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_thing], []);
    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_thing],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_thing]],
        &named_variables,
        1,
    ))];

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();

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
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_thing = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_thing, Vertex::Label(ANIMAL_LABEL), None)
        .unwrap()
        .clone();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_thing], []);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_thing],
        vec![ConstraintInstruction::Isa(IsaInstruction::new(isa, Inputs::None([]), &entry_annotations).map(&mapping))],
        vec![variable_positions[&var_thing]],
        &named_variables,
        1,
    ))];

    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();

    for row in &rows {
        let row = row.as_ref().unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }

    assert_eq!(rows.len(), 6);
}
