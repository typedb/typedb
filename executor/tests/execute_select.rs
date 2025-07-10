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
            instructions::{thing::HasInstruction, ConstraintInstruction, Inputs},
            planner::{
                conjunction_executable::{ConjunctionExecutable, ExecutionStep, IntersectionStep},
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
    match_executor::MatchExecutor, error::ReadExecutionError, pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow, ExecutionInterrupt,
};
use ir::{
    pattern::constraint::IsaKind,
    pipeline::{block::Block, ParameterRegistry},
    translation::PipelineTranslationContext,
};
use lending_iterator::LendingIterator;
use resource::profile::{CommitProfile, QueryProfile, StorageCounters};
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
const EMAIL_LABEL: Label = Label::new_static("email");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let person_owns_age = person_type
        .set_owns(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            age_type,
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();
    person_owns_age
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();
    let person_owns_name = person_type
        .set_owns(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            name_type,
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();
    person_owns_name
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();
    let email_type = type_manager.create_attribute_type(&mut snapshot, &EMAIL_LABEL).unwrap();
    email_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let person_owns_email = person_type
        .set_owns(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            email_type,
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();
    person_owns_email
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();

    let _person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    let _person_2 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    let _person_3 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();

    let mut _age_1 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(10)).unwrap();
    let mut _age_2 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(11)).unwrap();
    let mut _age_3 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(12)).unwrap();
    let mut _age_4 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(13)).unwrap();
    let mut _age_5 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(14)).unwrap();

    let mut _name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let mut _name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();
    let mut _name_3 = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned("Candice".to_string())))
        .unwrap();

    let mut _email_1 = thing_manager
        .create_attribute(&mut snapshot, email_type, Value::String(Cow::Owned("abc@email.com".to_string())))
        .unwrap();
    let mut _email_2 = thing_manager
        .create_attribute(&mut snapshot, email_type, Value::String(Cow::Owned("xyz@email.com".to_string())))
        .unwrap();

    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_age_1, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_age_2, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_age_3, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_name_1, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_name_2, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_email_1, StorageCounters::DISABLED).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, &_email_2, StorageCounters::DISABLED).unwrap();

    _person_2.set_has_unordered(&mut snapshot, &thing_manager, &_age_5, StorageCounters::DISABLED).unwrap();
    _person_2.set_has_unordered(&mut snapshot, &thing_manager, &_age_4, StorageCounters::DISABLED).unwrap();
    _person_2.set_has_unordered(&mut snapshot, &thing_manager, &_age_1, StorageCounters::DISABLED).unwrap();

    _person_3.set_has_unordered(&mut snapshot, &thing_manager, &_age_4, StorageCounters::DISABLED).unwrap();
    _person_3.set_has_unordered(&mut snapshot, &thing_manager, &_name_3, StorageCounters::DISABLED).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
    assert!(finalise_result.is_ok());
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn position_mapping<const N: usize, const M: usize>(
    row_vars: [Variable; N],
    internal_vars: [Variable; M],
) -> (HashMap<ExecutorVariable, Variable>, HashMap<Variable, VariablePosition>, HashMap<Variable, ExecutorVariable>) {
    let position_to_var: HashMap<_, _> =
        row_vars.into_iter().enumerate().map(|(i, v)| (ExecutorVariable::new_position(i as _), v)).collect();
    let variable_positions =
        HashMap::from_iter(position_to_var.iter().map(|(i, var)| (*var, i.as_position().unwrap())));
    let mapping: HashMap<_, _> = row_vars
        .into_iter()
        .map(|var| (var, ExecutorVariable::RowPosition(variable_positions[&var])))
        .chain(internal_vars.into_iter().map(|var| (var, ExecutorVariable::Internal(var))))
        .collect();
    (position_to_var, variable_positions, mapping)
}

#[test]
fn anonymous_vars_not_enumerated_or_counted() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has $_;

    // IR
    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_attribute_type = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_attribute = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute, None).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let block_annotations = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone(), None);
        let variable_registry = &translation_context.variable_registry;
        let previous_stage_variable_annotations = &BTreeMap::new();
        infer_types(
            &snapshot,
            &entry,
            variable_registry,
            &type_manager,
            previous_stage_variable_annotations,
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap()
    };
    let entry_annotations = block_annotations.type_annotations_of(entry.conjunction()).unwrap();

    let (row_vars, variable_positions, mapping) =
        position_mapping([var_person], [var_person_type, var_attribute, var_attribute_type]);
    let named_variables = HashSet::from([var_person, var_person_type].map(|var| mapping[&var]));

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_attribute, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_person]],
        &named_variables,
        1,
    ))];
    let executable =
        ConjunctionExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone(), None);
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

    // person1, <something>
    // person2, <something>
    // person3, <something>

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        println!("{}", r);
    }

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().multiplicity(), 1);
    assert_eq!(rows[1].as_ref().unwrap().multiplicity(), 1);
    assert_eq!(rows[2].as_ref().unwrap().multiplicity(), 1);
}

#[test]
fn unselected_named_vars_counted() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has $attr;
    //   select $person;

    // IR
    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_attribute_type = conjunction.constraints_mut().get_or_declare_variable("attr_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_attribute = conjunction.constraints_mut().get_or_declare_variable("attr", None).unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute, None).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let block_annotations = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone(), None);
        let variable_registry = &translation_context.variable_registry;
        let previous_stage_variable_annotations = &BTreeMap::new();
        infer_types(
            &snapshot,
            &entry,
            variable_registry,
            &type_manager,
            previous_stage_variable_annotations,
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap()
    };
    let entry_annotations = block_annotations.type_annotations_of(entry.conjunction()).unwrap();

    let (row_vars, variable_positions, mapping) =
        position_mapping([var_person], [var_attribute, var_person_type, var_attribute_type]);
    let named_variables =
        HashSet::from([var_person, var_attribute, var_person_type, var_attribute_type].map(|var| mapping[&var]));

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_attribute, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_person]],
        &named_variables,
        1,
    ))];

    let executable =
        ConjunctionExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone(), None);
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

    // 7x person 1, <something>
    // 3x person 2, <something>
    // 2x person 3, <something>

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        println!("{}", r);
    }

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().multiplicity(), 7);
    assert_eq!(rows[1].as_ref().unwrap().multiplicity(), 3);
    assert_eq!(rows[2].as_ref().unwrap().multiplicity(), 2);
}

#[test]
fn cartesian_named_counted_checked() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has name $name, has age $age, has email $_;
    //   select $person, $name;

    // IR
    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_name_type = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let var_age_type = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let var_email_type = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("name", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("age", None).unwrap();
    let var_email = conjunction.constraints_mut().create_anonymous_variable(None).unwrap();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name, None).unwrap().clone();
    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();
    let has_email = conjunction.constraints_mut().add_has(var_person, var_email, None).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_email, var_email_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_email_type, EMAIL_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let block_annotations = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone(), None);
        let variable_registry = &translation_context.variable_registry;
        let previous_stage_variable_annotations = &BTreeMap::new();
        infer_types(
            &snapshot,
            &entry,
            variable_registry,
            &type_manager,
            previous_stage_variable_annotations,
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap()
    };
    let entry_annotations = block_annotations.type_annotations_of(entry.conjunction()).unwrap();

    let (row_vars, variable_positions, mapping) = position_mapping(
        [var_person, var_age],
        [var_person_type, var_name_type, var_age_type, var_email_type, var_name, var_email],
    );
    let named_variables = HashSet::from([var_person, var_age, var_person_type, var_name].map(|var| mapping[&var]));

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![
            ConstraintInstruction::Has(
                HasInstruction::new(has_name, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
            ConstraintInstruction::Has(
                HasInstruction::new(has_age, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
            ConstraintInstruction::Has(
                HasInstruction::new(has_email, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
        ],
        vec![variable_positions[&var_person], variable_positions[&var_age]],
        &named_variables,
        2,
    ))];

    let conjunction_executable =
        ConjunctionExecutable::new(next_executable_id(), steps, variable_positions, row_vars, PlannerStatistics::new());

    // Executor
    let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone(), None);
    let executor = MatchExecutor::new(
        &conjunction_executable,
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

    // 2x person 1, age_1, <name something>, <email something>
    // 2x person 1, age_2, <name something>, <email something>
    // 2x person 1, age_3, <name something>, <email something>

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        print!("{}", r);
    }

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().multiplicity(), 2);
    assert_eq!(rows[1].as_ref().unwrap().multiplicity(), 2);
    assert_eq!(rows[2].as_ref().unwrap().multiplicity(), 2);
}
