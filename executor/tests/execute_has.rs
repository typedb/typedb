/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compiler::{
    annotation::{
        function::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        match_inference::infer_types,
    },
    executable::match_::{
        instructions::{
            thing::{HasInstruction, HasReverseInstruction, IsaInstruction},
            ConstraintInstruction, Inputs,
        },
        planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
        },
    },
    ExecutorVariable, VariablePosition,
};
use concept::{
    thing::object::ObjectAPI,
    type_::{annotation::AnnotationCardinality, owns::OwnsAnnotation, Ordering, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, row::MaybeOwnedRow,
    ExecutionInterrupt,
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
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_owns_age = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

    let age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
    let age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();
    let age_3 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap();
    let age_4 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap();
    let age_5 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap();

    let name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();
    let name_3 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Candice".to_string())))
        .unwrap();

    person_1.set_has_unordered(&mut snapshot, &thing_manager, age_1.clone()).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, age_2.clone()).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, age_3.clone()).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, name_1.clone()).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, name_2.clone()).unwrap();

    person_2.set_has_unordered(&mut snapshot, &thing_manager, age_5.clone()).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, age_4.clone()).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, age_1.clone()).unwrap();

    person_3.set_has_unordered(&mut snapshot, &thing_manager, age_4.clone()).unwrap();
    person_3.set_has_unordered(&mut snapshot, &thing_manager, name_3.clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
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
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

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
    let row_vars = vec![var_person, var_age];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_person, var_age, var_age_type, var_person_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

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
    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(snapshot);
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
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();
    let var_person_1 = conjunction.get_or_declare_variable("person-1").unwrap();
    let var_person_2 = conjunction.get_or_declare_variable("person-2").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();

    let isa_person_1 =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_1, var_person_type.into()).unwrap().clone();
    let has_name_1 = conjunction.constraints_mut().add_has(var_person_1, var_name).unwrap().clone();
    let has_name_2 = conjunction.constraints_mut().add_has(var_person_2, var_name).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_1, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_2, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let entry = builder.finish().unwrap();
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

    let row_vars = vec![var_person_1, var_person_2, var_name];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_person_1, var_person_2, var_name, var_person_type, var_name_type].map(|var| {
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
    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(snapshot);
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
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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

    let row_vars = vec![var_person, var_name, var_age];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping =
        HashMap::from([var_person, var_name, var_age, var_person_type, var_name_type, var_age_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }));
    let named_variables = mapping.values().copied().collect();

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
    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(snapshot);
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
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable("attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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

    let row_vars = vec![var_person, var_attribute];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_person, var_attribute, var_person_type].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

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
    let executable = MatchExecutable::new(steps, variable_positions.clone(), row_vars);

    // Executor
    let snapshot = Arc::new(snapshot);
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
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
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

    let row_vars = vec![var_person, var_age, var_person_type, var_age_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_person, var_age].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

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
    let executable = MatchExecutable::new(steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(snapshot);
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
