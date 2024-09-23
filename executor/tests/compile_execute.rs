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

use compiler::match_::{
    inference::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_inference::infer_types_for_match_block,
    },
    planner::{pattern_plan::MatchProgram, program_plan::ProgramPlan},
};
use concept::{
    thing::{object::ObjectAPI, statistics::Statistics},
    type_::{
        annotation::AnnotationCardinality, owns::OwnsAnnotation, relates::RelatesAnnotation, Ordering, OwnerAPI,
        PlayerAPI,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{pipeline::stage::ExecutionContext, program_executor::ProgramExecutor, ExecutionInterrupt};
use ir::{
    program::function_signature::HashMapFunctionSignatureIndex,
    translation::{match_::translate_match, TranslationContext},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{sequence_number::SequenceNumber, snapshot::CommittableSnapshot};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

#[test]
fn test_has_planning_traversal() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let mut snapshot = storage.clone().open_snapshot_write();

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();

    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let person_owns_age = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();

    let person = [(); 3].map(|()| thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap());

    let age = [10, 11, 12, 13, 14]
        .map(|age| thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(age)).unwrap());

    let name = ["John", "Alice", "Leila"].map(|name| {
        thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed(name))).unwrap()
    });

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
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let entry_annotations = infer_types_for_match_block(
        &block,
        &translation_context.variable_registry,
        &*snapshot,
        &type_manager,
        &BTreeMap::new(),
        &IndexedAnnotatedFunctions::empty(),
        &AnnotatedUnindexedFunctions::empty(),
    )
    .unwrap();

    let pattern_plan = MatchProgram::compile(
        &block,
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

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
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let mut snapshot = storage.clone().open_snapshot_write();

    const CARDINALITY_ANY: AnnotationCardinality = AnnotationCardinality::new(0, None);
    const OWNS_CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(CARDINALITY_ANY);
    const RELATES_CARDINALITY_ANY: RelatesAnnotation = RelatesAnnotation::Cardinality(CARDINALITY_ANY);

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let membership_type = type_manager.create_relation_type(&mut snapshot, &MEMBERSHIP_LABEL).unwrap();

    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let person_owns_name = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered)
        .unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, OWNS_CARDINALITY_ANY).unwrap();

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

    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type.clone()).unwrap();

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
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let entry_annotations = infer_types_for_match_block(
        &block,
        &translation_context.variable_registry,
        &*snapshot,
        &type_manager,
        &BTreeMap::new(),
        &IndexedAnnotatedFunctions::empty(),
        &AnnotatedUnindexedFunctions::empty(),
    )
    .unwrap();

    let pattern_plan = MatchProgram::compile(
        &block,
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

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

const USER_LABEL: Label = Label::new_static("user");
const ORDER_LABEL: Label = Label::new_static("order");
const PURCHASE_LABEL: Label = Label::new_static("purchase");
const PURCHASE_BUYER_LABEL: Label = Label::new_static_scoped("buyer", "purchase", "purchase:buyer");
const PURCHASE_ORDER_LABEL: Label = Label::new_static_scoped("order", "purchase", "purchase:order");
const STATUS_LABEL: Label = Label::new_static("status");
const TIMESTAMP_LABEL: Label = Label::new_static("timestamp");

#[test]
fn test_links_intersection() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let mut snapshot = storage.clone().open_snapshot_write();

    const CARDINALITY_ANY: AnnotationCardinality = AnnotationCardinality::new(0, None);
    const RELATES_CARDINALITY_ANY: RelatesAnnotation = RelatesAnnotation::Cardinality(CARDINALITY_ANY);

    let user_type = type_manager.create_entity_type(&mut snapshot, &USER_LABEL).unwrap();
    let order_type = type_manager.create_entity_type(&mut snapshot, &ORDER_LABEL).unwrap();
    let purchase_type = type_manager.create_relation_type(&mut snapshot, &PURCHASE_LABEL).unwrap();

    let status_type = type_manager.create_attribute_type(&mut snapshot, &STATUS_LABEL).unwrap();
    status_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let timestamp_type = type_manager.create_attribute_type(&mut snapshot, &TIMESTAMP_LABEL).unwrap();
    timestamp_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::DateTime).unwrap();

    order_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, status_type.clone(), Ordering::Unordered)
        .unwrap();
    order_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, timestamp_type.clone(), Ordering::Unordered)
        .unwrap();

    let relates_buyer = purchase_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            PURCHASE_BUYER_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    relates_buyer.set_annotation(&mut snapshot, &type_manager, &thing_manager, RELATES_CARDINALITY_ANY).unwrap();
    let purchase_buyer_type = relates_buyer.role();

    user_type.set_plays(&mut snapshot, &type_manager, &thing_manager, purchase_buyer_type.clone()).unwrap();

    let relates_order = purchase_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            PURCHASE_ORDER_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    relates_order.set_annotation(&mut snapshot, &type_manager, &thing_manager, RELATES_CARDINALITY_ANY).unwrap();
    let purchase_order_type = relates_order.role();

    order_type.set_plays(&mut snapshot, &type_manager, &thing_manager, purchase_order_type.clone()).unwrap();

    // END SCHEMA

    let user = [(); 3].map(|_| thing_manager.create_entity(&mut snapshot, user_type.clone()).unwrap());
    let order = [(); 3].map(|_| thing_manager.create_entity(&mut snapshot, order_type.clone()).unwrap());

    let status = ["canceled", "dispatched", "paid"]
        .map(|s| thing_manager.create_attribute(&mut snapshot, status_type.clone(), Value::String(s.into())).unwrap());

    let timestamp = [(); 3].map(|_| {
        thing_manager
            .create_attribute(&mut snapshot, timestamp_type.clone(), Value::DateTime(Default::default()))
            .unwrap()
    });

    order[0].set_has_unordered(&mut snapshot, &thing_manager, status[0].clone()).unwrap();
    order[1].set_has_unordered(&mut snapshot, &thing_manager, status[1].clone()).unwrap();
    order[2].set_has_unordered(&mut snapshot, &thing_manager, status[2].clone()).unwrap();

    order[0].set_has_unordered(&mut snapshot, &thing_manager, timestamp[0].clone()).unwrap();
    order[1].set_has_unordered(&mut snapshot, &thing_manager, timestamp[1].clone()).unwrap();
    order[2].set_has_unordered(&mut snapshot, &thing_manager, timestamp[2].clone()).unwrap();

    let purchase = [(); 3].map(|_| thing_manager.create_relation(&mut snapshot, purchase_type.clone()).unwrap());

    purchase[0]
        .add_player(&mut snapshot, &thing_manager, purchase_buyer_type.clone(), user[0].clone().into_owned_object())
        .unwrap();
    purchase[1]
        .add_player(&mut snapshot, &thing_manager, purchase_buyer_type.clone(), user[0].clone().into_owned_object())
        .unwrap();
    purchase[2]
        .add_player(&mut snapshot, &thing_manager, purchase_buyer_type.clone(), user[1].clone().into_owned_object())
        .unwrap();

    purchase[0]
        .add_player(&mut snapshot, &thing_manager, purchase_order_type.clone(), order[0].clone().into_owned_object())
        .unwrap();
    purchase[1]
        .add_player(&mut snapshot, &thing_manager, purchase_order_type.clone(), order[0].clone().into_owned_object())
        .unwrap();
    purchase[2]
        .add_player(&mut snapshot, &thing_manager, purchase_order_type.clone(), order[1].clone().into_owned_object())
        .unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    // END DATA

    let query = "match
    $p isa purchase, links (order: $order, buyer: $buyer);
    $order has status $status;
    $order has timestamp $timestamp;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["user", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let entry_annotations = infer_types_for_match_block(
        &block,
        &translation_context.variable_registry,
        &*snapshot,
        &type_manager,
        &BTreeMap::new(),
        &IndexedAnnotatedFunctions::empty(),
        &AnnotatedUnindexedFunctions::empty(),
    )
    .unwrap();

    let pattern_plan = MatchProgram::compile(
        &block,
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .into_iter()
        .try_collect::<_, Vec<_>, _>()
        .unwrap();

    assert_eq!(rows.len(), 3);

    for row in rows {
        for value in row {
            print!("{}, ", value);
        }
        println!()
    }
}
