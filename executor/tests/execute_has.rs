/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::{
    inference::{annotated_functions::AnnotatedCommittedFunctions, annotated_program, type_inference::infer_types},
    instruction::constraint::instructions::{ConstraintInstruction, HasInstruction, HasReverseInstruction, Inputs},
    planner::{
        pattern_plan::{IntersectionStep, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};
use concept::{
    error::ConceptReadError,
    thing::object::ObjectAPI,
    type_::{annotation::AnnotationCardinality, owns::OwnsAnnotation, Ordering, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{batch::ImmutableRow, program_executor::ProgramExecutor};
use ir::{
    pattern::constraint::IsaKind,
    program::{block::FunctionalBlock, program::Program},
};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};

use crate::common::{load_managers, setup_storage};

mod common;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, CARDINALITY_ANY).unwrap();

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
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person isa person, has age $age;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.scoped_name().as_str()).unwrap();
    block.add_limit(3);
    let filter = block.add_filter(vec!["person", "age"]).unwrap().clone();

    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_person,
        vec![ConstraintInstruction::Has(HasInstruction::new(
            has_age,
            Inputs::None([]),
            annotated_program.entry_annotations(),
        ))],
        &[var_person, var_age],
    ))];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.get_multiplicity(), 1);
        print!("{}", r);
    }
}

#[test]
fn traverse_has_bounded_sorted_from_chain_intersect() {
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person-1 has name $name;
    //    $person-2 has name $name; # reverse!

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();
    let var_person_1 = conjunction.get_or_declare_variable("person-1").unwrap();
    let var_person_2 = conjunction.get_or_declare_variable("person-2").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();

    let isa_person_1 =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_1, var_person_type).unwrap().clone();
    let has_name_1 = conjunction.constraints_mut().add_has(var_person_1, var_name).unwrap().clone();
    let has_name_2 = conjunction.constraints_mut().add_has(var_person_2, var_name).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_1, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person_2, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.scoped_name().as_str()).unwrap();
    block.add_limit(3);

    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![
        Step::Intersection(IntersectionStep::new(
            var_person_1,
            vec![ConstraintInstruction::IsaReverse(isa_person_1, Inputs::None([]))],
            &[var_person_1],
        )),
        Step::Intersection(IntersectionStep::new(
            var_name,
            vec![
                ConstraintInstruction::Has(HasInstruction::new(
                    has_name_1,
                    Inputs::Single([var_person_1]),
                    annotated_program.entry_annotations(),
                )),
                ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                    has_name_2,
                    Inputs::None([]),
                    annotated_program.entry_annotations(),
                )),
            ],
            &[var_person_1, var_person_2, var_name],
        )),
    ];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 3); // $person-1 is $person-2, one per name

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.get_multiplicity(), 1);
        print!("{}", r);
    }
}

#[test]
fn traverse_has_unbounded_sorted_from_intersect() {
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person has name $name, has age $age;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.scoped_name().as_str()).unwrap();
    block.add_limit(3);
    let filter = block.add_filter(vec!["person", "age"]).unwrap().clone();

    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_person,
        vec![
            ConstraintInstruction::Has(HasInstruction::new(
                has_age,
                Inputs::None([]),
                annotated_program.entry_annotations(),
            )),
            ConstraintInstruction::Has(HasInstruction::new(
                has_name,
                Inputs::None([]),
                annotated_program.entry_annotations(),
            )),
        ],
        &[var_person, var_name, var_age],
    ))];
    // TODO: incorporate the filter
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.get_multiplicity(), 1);
        print!("{}", r);
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
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable("attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_attribute,
        vec![ConstraintInstruction::Has(HasInstruction::new(
            has_attribute,
            Inputs::None([]),
            annotated_program.entry_annotations(),
        ))],
        &[var_person, var_attribute],
    ))];
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let variable_positions = executor.entry_variable_positions().clone();

    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();

    // person 1 - has age 1, has age 2, has age 3, has name 1, has name 2 => 5 answers
    // person 2 - has age 1, has age 4, has age 5 => 3 answers
    // person 3 - has age 4, has name 3 => 2 answers

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        assert_eq!(r.get_multiplicity(), 1);
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
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person has age $age;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_age_type = conjunction.get_or_declare_variable("age_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.scoped_name().as_str()).unwrap();

    let program = Program::new(block.finish(), Vec::new());

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_age,
        vec![ConstraintInstruction::HasReverse(HasReverseInstruction::new(
            has_age,
            Inputs::None([]),
            annotated_program.entry_annotations(),
        ))],
        &[var_person, var_age],
    ))];
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();
    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.clone().into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 7);

    for row in rows {
        let r = row.unwrap();
        assert_eq!(r.get_multiplicity(), 1);
        print!("{}", r);
    }
}