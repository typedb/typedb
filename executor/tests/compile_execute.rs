/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::match_::{
    inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
    planner::{pattern_plan::PatternPlan, program_plan::ProgramPlan},
};
use concept::{
    thing::{object::ObjectAPI, statistics::Statistics},
    type_::{
        annotation::AnnotationCardinality, owns::OwnsAnnotation, relates::RelatesAnnotation, Ordering, OwnerAPI,
        PlayerAPI,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::program_executor::ProgramExecutor;
use ir::{program::function_signature::HashMapFunctionSignatureIndex, translation::match_::translate_match};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{sequence_number::SequenceNumber, snapshot::CommittableSnapshot};

use crate::common::{load_managers, setup_storage};

mod common;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

#[test]
fn test_has_planning_traversal() {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();

    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person = [
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
    ];

    let age = [
        thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap(),
        thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap(),
        thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap(),
        thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap(),
        thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap(),
    ];

    let name = [
        thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("John"))).unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Alice")))
            .unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Leila")))
            .unwrap(),
    ];

    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[1].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[2].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();

    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[4].clone()).unwrap();
    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();

    person[2].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    let query = "match $person isa person, has name $name, has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let builder = translate_match(&empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, annotated_functions) =
        infer_types(&block, vec![], &snapshot, &type_manager, &IndexedAnnotatedFunctions::empty()).unwrap();
    let pattern_plan = PatternPlan::from_block(&block, &entry_annotations, &HashMap::new(), &statistics);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();
    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .into_iter()
        .try_collect::<_, Vec<_>, _>()
        .unwrap();

    assert_eq!(rows.len(), 7);

    for row in rows {
        for value in row {
            print!("{}, ", value);
        }
        println!()
    }
}

#[test]
fn test_links_planning_traversal() {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    const CARDINALITY_ANY: AnnotationCardinality = AnnotationCardinality::new(0, None);
    const OWNS_CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(CARDINALITY_ANY);
    const RELATES_CARDINALITY_ANY: RelatesAnnotation = RelatesAnnotation::Cardinality(CARDINALITY_ANY);

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let membership_type = type_manager.create_relation_type(&mut snapshot, &MEMBERSHIP_LABEL).unwrap();

    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, OWNS_CARDINALITY_ANY).unwrap();

    let relates_member = membership_type
        .create_relates(&mut snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name().as_str(), Ordering::Unordered)
        .unwrap();
    relates_member.set_annotation(&mut snapshot, &type_manager, &thing_manager, RELATES_CARDINALITY_ANY).unwrap();
    let membership_member_type = relates_member.role();

    person_type.set_plays(&mut snapshot, &type_manager, membership_member_type.clone()).unwrap();

    let person = [
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
    ];

    let membership = [
        thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap(),
        thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap(),
    ];

    let name = [
        thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("John"))).unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Alice")))
            .unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Leila")))
            .unwrap(),
    ];

    person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
    person[1].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();
    person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();

    membership[0]
        .add_player(
            &mut snapshot,
            &thing_manager,
            membership_member_type.clone(),
            person[0].clone().into_owned_object(),
        )
        .unwrap();
    membership[1]
        .add_player(
            &mut snapshot,
            &thing_manager,
            membership_member_type.clone(),
            person[2].clone().into_owned_object(),
        )
        .unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    let query = "match $person isa person, has name $name; $membership isa membership, links ($person);";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let builder = translate_match(&empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) =
        infer_types(&block, vec![], &snapshot, &type_manager, &IndexedAnnotatedFunctions::empty()).unwrap();
    let pattern_plan = PatternPlan::from_block(&block, &entry_annotations, &HashMap::new(), &statistics);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();
    let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));

    let rows = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .into_iter()
        .try_collect::<_, Vec<_>, _>()
        .unwrap();

    assert_eq!(rows.len(), 2);

    for row in rows {
        for value in row {
            print!("{}, ", value);
        }
        println!()
    }
}
