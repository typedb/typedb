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

use answer::variable::Variable;
use compiler::{
    annotation::{function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types},
    executable::{
        function::ExecutableFunctionRegistry,
        match_::{
            instructions::{
                thing::{HasInstruction, HasReverseInstruction, IsaInstruction},
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
use concept::{
    thing::object::ObjectAPI,
    type_::{annotation::AnnotationCardinality, owns::OwnsAnnotation, Ordering, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, profile::QueryProfile,
    row::MaybeOwnedRow, ExecutionInterrupt,
};
use ir::{
    pattern::constraint::IsaKind,
    pipeline::{block::Block, ParameterRegistry},
    translation::TranslationContext,
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered).unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    let person_2 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    let person_3 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();

    let age_1 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(10)).unwrap();
    let age_2 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(11)).unwrap();
    let age_3 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(12)).unwrap();
    let age_4 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(13)).unwrap();
    let age_5 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(14)).unwrap();

    let name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();
    let name_3 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Candice".to_string())))
        .unwrap();

    person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_1).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_2).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_3).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &name_1).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &name_2).unwrap();

    person_2.set_has_unordered(&mut snapshot, &thing_manager, &age_5).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &age_4).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &age_1).unwrap();

    person_3.set_has_unordered(&mut snapshot, &thing_manager, &age_4).unwrap();
    person_3.set_has_unordered(&mut snapshot, &thing_manager, &name_3).unwrap();

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
fn traverse_has_unbounded_sorted_from() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has age $age;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("age_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("age", None).unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();
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
        position_mapping([var_person, var_age], [var_age_type, var_person_type]);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_age, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_person], variable_positions[&var_age]],
        &named_variables,
        2,
    ))];
    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(snapshot);
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

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.multiplicity(), 1);
        print!("{}", r);
    }
}

#[test]
fn traverse_has_bounded_sorted_from_chain_intersect() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    // query:
    //   match
    //    $person-1 has name $name;
    //    $person-2 has name $name; # reverse!

    // IR
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_name_type = conjunction.constraints_mut().get_or_declare_variable("name_type", None).unwrap();
    let var_person_1 = conjunction.constraints_mut().get_or_declare_variable("person-1", None).unwrap();
    let var_person_2 = conjunction.constraints_mut().get_or_declare_variable("person-2", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("name", None).unwrap();

    let isa_person_1 = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person_1, var_person_type.into(), None)
        .unwrap()
        .clone();
    let has_name_1 = conjunction.constraints_mut().add_has(var_person_1, var_name, None).unwrap().clone();
    let has_name_2 = conjunction.constraints_mut().add_has(var_person_2, var_name, None).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_1, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_2, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let entry = builder.finish().unwrap();
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
        position_mapping([var_person_1, var_person_2, var_name], [var_person_type, var_name_type]);

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_1],
            vec![ConstraintInstruction::Isa(
                IsaInstruction::new(isa_person_1, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person_1]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_name],
            vec![
                ConstraintInstruction::Has(
                    HasInstruction::new(has_name_1, Inputs::Single([var_person_1]), &entry_annotations).map(&mapping),
                ),
                ConstraintInstruction::HasReverse(
                    HasReverseInstruction::new(has_name_2, Inputs::None([]), &entry_annotations).map(&mapping),
                ),
            ],
            vec![variable_positions[&var_person_1], variable_positions[&var_person_2], variable_positions[&var_name]],
            &named_variables,
            3,
        )),
    ];
    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(snapshot);
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

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    assert_eq!(rows.len(), 3); // $person-1 is $person-2, one per name

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.multiplicity(), 1);
        print!("{}", r);
    }
}

#[test]
fn traverse_has_unbounded_sorted_from_intersect() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has name $name, has age $age;

    // IR

    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("age_type", None).unwrap();
    let var_name_type = conjunction.constraints_mut().get_or_declare_variable("name_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("age", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("name", None).unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name, None).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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
        position_mapping([var_person, var_name, var_age], [var_person_type, var_name_type, var_age_type]);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![
            ConstraintInstruction::Has(
                HasInstruction::new(has_age, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
            ConstraintInstruction::Has(
                HasInstruction::new(has_name, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
        ],
        vec![variable_positions[&var_person], variable_positions[&var_name], variable_positions[&var_age]],
        &named_variables,
        3,
    ))];
    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(snapshot);
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

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.multiplicity(), 1);
        print!("{}", r);
    }
}

#[test]
fn traverse_has_unbounded_sorted_to_merged() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has $attribute;

    // IR

    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_attribute = conjunction.constraints_mut().get_or_declare_variable("attr", None).unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute, None).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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
        position_mapping([var_person, var_attribute], [var_person_type]);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_attribute],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_attribute, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_person], variable_positions[&var_attribute]],
        &named_variables,
        2,
    ))];
    let executable = MatchExecutable::new(
        next_executable_id(),
        steps,
        variable_positions.clone(),
        row_vars,
        PlannerStatistics::new(),
    );

    // Executor
    let snapshot = Arc::new(snapshot);
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

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();

    // person 1 - has age 1, has age 2, has age 3, has name 1, has name 2 => 5 answers
    // person 2 - has age 1, has age 4, has age 5 => 3 answers
    // person 3 - has age 4, has name 3 => 2 answers

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        assert_eq!(r.multiplicity(), 1);
        print!("{}", r);
    }
    assert_eq!(rows.len(), 10);

    // sort ordering check
    let attribute_position = *variable_positions.get(&var_attribute).unwrap();
    let mut previous_attribute = rows[0].as_ref().unwrap().get(attribute_position);
    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        let attribute = r.get(attribute_position);
        assert!(previous_attribute <= attribute, "{} <= {} failed", &previous_attribute, &attribute);
        previous_attribute = attribute;
    }
}

#[test]
fn traverse_has_reverse_unbounded_sorted_from() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has age $age;

    // IR

    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("age_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("age", None).unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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
        position_mapping([var_person, var_age, var_person_type, var_age_type], []);

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_age],
        vec![ConstraintInstruction::HasReverse(
            HasReverseInstruction::new(has_age, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_person], variable_positions[&var_age]],
        &named_variables,
        2,
    ))];
    let executable =
        MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(snapshot);
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

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.multiplicity(), 1);
        print!("{}", r);
    }
}
