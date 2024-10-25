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
            thing::{IsaReverseInstruction, LinksInstruction, LinksReverseInstruction},
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
    type_::{
        annotation::AnnotationCardinality, owns::OwnsAnnotation, relates::RelatesAnnotation, Ordering, OwnerAPI,
        PlayerAPI,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use ir::{pattern::constraint::IsaKind, pipeline::block::Block, translation::TranslationContext};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const GROUP_LABEL: Label = Label::new_static("group");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");
const MEMBERSHIP_GROUP_LABEL: Label = Label::new_static_scoped("group", "membership", "membership:group");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

const CARDINALITY_ANY: AnnotationCardinality = AnnotationCardinality::new(0, None);
const OWNS_CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(CARDINALITY_ANY);
const RELATES_CARDINALITY_ANY: RelatesAnnotation = RelatesAnnotation::Cardinality(CARDINALITY_ANY);

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, &GROUP_LABEL).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, &MEMBERSHIP_LABEL).unwrap();

    let relates_member = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_MEMBER_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    relates_member.set_annotation(&mut snapshot, &type_manager, &thing_manager, RELATES_CARDINALITY_ANY).unwrap();
    let membership_member_type = relates_member.role();

    let relates_group = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_GROUP_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    relates_group.set_annotation(&mut snapshot, &type_manager, &thing_manager, RELATES_CARDINALITY_ANY).unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let person_owns_age = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, OWNS_CARDINALITY_ANY).unwrap();

    let person_owns_name = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, OWNS_CARDINALITY_ANY).unwrap();

    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_group_type.clone()).unwrap();

    /*
    insert
         $person_1 isa person, has age 10, has age 11;
         $person_2 isa person, has age 10;
         $person_3 isa person, has name "Abby", has name "Bobby";

         $group_1 isa group;
         $group_2 isa group;

         $membership_1 isa membership, links(member: $person_1, group: $group_1);
         $membership_2 isa membership, links(member: $person_3, group: $group_2);
    */

    let person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

    let group_1 = thing_manager.create_entity(&mut snapshot, group_type.clone()).unwrap();
    let group_2 = thing_manager.create_entity(&mut snapshot, group_type.clone()).unwrap();

    let membership_1 = thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap();
    let membership_2 = thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap();

    let age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
    let age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();

    let name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();

    membership_1
        .add_player(&mut snapshot, &thing_manager, membership_member_type.clone(), person_1.clone().into_owned_object())
        .unwrap();
    membership_1
        .add_player(&mut snapshot, &thing_manager, membership_group_type.clone(), group_1.clone().into_owned_object())
        .unwrap();
    membership_2
        .add_player(&mut snapshot, &thing_manager, membership_member_type.clone(), person_3.clone().into_owned_object())
        .unwrap();
    membership_2
        .add_player(&mut snapshot, &thing_manager, membership_group_type.clone(), group_2.clone().into_owned_object())
        .unwrap();

    person_1.set_has_unordered(&mut snapshot, &thing_manager, age_1.as_reference()).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, age_2.as_reference()).unwrap();

    person_2.set_has_unordered(&mut snapshot, &thing_manager, age_1.as_reference()).unwrap();

    person_3.set_has_unordered(&mut snapshot, &thing_manager, name_1.as_reference()).unwrap();
    person_3.set_has_unordered(&mut snapshot, &thing_manager, name_2.as_reference()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok(), "{:?}", finalise_result.unwrap_err());
    snapshot.commit().unwrap();
}

#[test]
fn traverse_links_unbounded_sorted_from() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_group_type = conjunction.get_or_declare_variable("group_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();
    let var_membership_group_type = conjunction.get_or_declare_variable("membership_group_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_group = conjunction.get_or_declare_variable("group").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();
    let links_membership_group =
        conjunction.constraints_mut().add_links(var_membership, var_group, var_membership_group_type).unwrap().clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_group, var_group_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_group_type, GROUP_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_group_type, MEMBERSHIP_GROUP_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_membership, var_group, var_person];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [
            var_membership,
            var_group,
            var_person,
            var_membership_type,
            var_group_type,
            var_person_type,
            var_membership_group_type,
            var_membership_member_type,
        ]
        .map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_membership],
        vec![
            ConstraintInstruction::Links(
                LinksInstruction::new(links_membership_person, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
            ConstraintInstruction::Links(
                LinksInstruction::new(links_membership_group, Inputs::None([]), &entry_annotations).map(&mapping),
            ),
        ],
        vec![variable_positions[&var_membership], variable_positions[&var_group], variable_positions[&var_person]],
        &named_variables,
        3,
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_unbounded_sorted_to() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_membership, var_person];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_person, var_person_type, var_membership_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![ConstraintInstruction::Links(
            LinksInstruction::new(links_membership_person, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_membership], variable_positions[&var_person]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_bounded_relation() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    let isa_membership = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_membership, var_membership_type.into())
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_membership, var_person];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_membership, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_membership]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![ConstraintInstruction::Links(
                LinksInstruction::new(links_membership_person, Inputs::Single([var_membership]), &entry_annotations)
                    .map(&mapping),
            )],
            vec![variable_positions[&var_person]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_bounded_relation_player() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    let isa_person =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap().clone();
    let isa_membership = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_membership, var_membership_type.into())
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_membership, var_person, var_membership_member_type];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_membership, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_membership]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_membership], variable_positions[&var_person]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership_member_type],
            vec![ConstraintInstruction::Links(
                LinksInstruction::new(
                    links_membership_person,
                    Inputs::Dual([var_membership, var_person]),
                    &entry_annotations,
                )
                .map(&mapping),
            )],
            vec![variable_positions[&var_membership_member_type]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        println!("{}", r);
    }
}

#[test]
fn traverse_links_reverse_unbounded_sorted_from() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

    let entry = builder.finish();

    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let snapshot1 = &*snapshot;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        snapshot1,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_membership, var_person];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_person],
        vec![ConstraintInstruction::LinksReverse(
            LinksReverseInstruction::new(links_membership_person, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_membership], variable_positions[&var_person]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_reverse_unbounded_sorted_to() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

    let entry = builder.finish();

    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let snapshot1 = &*snapshot;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        snapshot1,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let row_vars = vec![var_membership, var_person];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_membership],
        vec![ConstraintInstruction::LinksReverse(
            LinksReverseInstruction::new(links_membership_person, Inputs::None([]), &entry_annotations).map(&mapping),
        )],
        vec![variable_positions[&var_membership], variable_positions[&var_person]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_reverse_bounded_player() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    let isa_person =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_person, var_membership];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership],
            vec![ConstraintInstruction::LinksReverse(
                LinksReverseInstruction::new(links_membership_person, Inputs::Single([var_person]), &entry_annotations)
                    .map(&mapping),
            )],
            vec![variable_positions[&var_membership]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_links_reverse_bounded_player_relation() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable("membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable("membership_member_type").unwrap();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_membership = conjunction.get_or_declare_variable("membership").unwrap();

    let links_membership_person = conjunction
        .constraints_mut()
        .add_links(var_membership, var_person, var_membership_member_type)
        .unwrap()
        .clone();

    let isa_person =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap().clone();
    let isa_membership = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_membership, var_membership_type.into())
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();

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

    let row_vars = vec![var_person, var_membership];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [var_membership, var_membership_type, var_person, var_person_type, var_membership_member_type].map(|var| {
            if row_vars.contains(&var) {
                (var, ExecutorVariable::RowPosition(variable_positions[&var]))
            } else {
                (var, ExecutorVariable::Internal(var))
            }
        }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_membership, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_membership]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_membership_member_type],
            vec![ConstraintInstruction::LinksReverse(
                LinksReverseInstruction::new(
                    links_membership_person,
                    Inputs::Dual([var_membership, var_person]),
                    &entry_annotations,
                )
                .map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_membership]],
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
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let r = row.unwrap();
        println!("{}", r);
    }
}
