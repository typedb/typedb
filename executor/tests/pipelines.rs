/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::{
    thing::{object::ObjectAPI, statistics::Statistics, thing_manager::ThingManager},
    type_::{OwnerAPI, type_manager::TypeManager},
};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::pipeline::StageAPI;
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::{
    durability_client::WALClient, MVCCStorage, snapshot::CommittableSnapshot,
};

use crate::common::{load_managers, setup_storage};

mod common;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    statistics: Statistics,
}

fn setup_common() -> (Context, ThingManager) {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();
    let schema = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..), plays membership:member;
        relation membership relates member;
    "#;
    storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    let seq = snapshot.commit().unwrap();
    let mut statistics = Statistics::new(seq.unwrap());
    statistics.may_synchronise(&storage).unwrap();

    (Context { storage, type_manager, function_manager, query_manager, statistics }, thing_manager)
}

#[test]
fn test_insert() {
    let (context, thing_manager) = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let mut pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            thing_manager,
            &context.function_manager,
            &context.statistics,
            &IndexedAnnotatedFunctions::empty(),
            &query,
        )
        .unwrap();
    let (mut iterator, snapshot, thing_manager) = pipeline.into_iterator().unwrap();
    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();
    let thing_manager = Arc::into_inner(thing_manager).unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 = thing_manager.get_attribute_with_value(
            &snapshot, age_type, Value::Long(10)
        ).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_dummy_match() {
    todo!("This hits a todo");
    // let (mut context, thing_manager) = setup_common();
    //
    // let snapshot = context.storage.open_snapshot_write();
    // let query = "match ($p) isa membership;";
    // // let query = "match $person isa person, has name $name, has age $age;";
    // let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    // // // Executor
    // let mut pipeline = context
    //     .query_manager
    //     .prepare_write_pipeline(
    //         snapshot,
    //         &context.type_manager,
    //         thing_manager,
    //         &context.function_manager,
    //         &context.statistics,
    //         &IndexedAnnotatedFunctions::empty(),
    //         &match_,
    //     )
    //     .unwrap();
    // let mut rows = Vec::new();
    // while let Some(row) = pipeline.next() {
    //     rows.push(row.unwrap().clone().into_owned());
    // }
}

#[test]
fn test_match_as_pipeline() {
    todo!("This hits a todo");
    // let (mut context, thing_manager) = setup_common();
    //
    // let mut snapshot = context.storage.clone().open_snapshot_write();
    // let query_str = r#"
    // insert
    //     $p0 isa person, has age 10, has age 11, has age 12, has name "John", has name "Alice";
    //     $p1 isa person, has age 14, has age 13, has  age 10;
    //     $p2 isa person, has age 13, has name "Leila";
    //
    // "#;
    // let insert = typeql::parse_query(query_str).unwrap().into_pipeline();
    // let mut insert_pipeline = context
    //     .query_manager
    //     .prepare_write_pipeline(
    //         snapshot,
    //         &context.type_manager,
    //         thing_manager,
    //         &context.function_manager,
    //         &context.statistics,
    //         &IndexedAnnotatedFunctions::empty(),
    //         &insert,
    //     )
    //     .unwrap();
    // let mut count = 0;
    // while let Some(result) = insert_pipeline.next() {
    //     assert!(result.is_ok(), "{:?}", result);
    //     count += 1;
    // }
    // let PipelineContext::Owned(mut snapshot, mut thing_manager) = insert_pipeline.finalise_and_into_context().unwrap()
    // else {
    //     unreachable!()
    // };
    // let inserted_seq = snapshot.commit().unwrap();
    //
    // let mut newer_statistics = Statistics::new(inserted_seq.unwrap());
    // newer_statistics.may_synchronise(&context.storage).unwrap();
    // let mut snapshot = context.storage.open_snapshot_write();
    // let query = "match $person isa person, has name $name, has age $age;";
    // let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    // // // Executor
    // let mut pipeline = context
    //     .query_manager
    //     .prepare_write_pipeline(
    //         snapshot,
    //         &context.type_manager,
    //         thing_manager,
    //         &context.function_manager,
    //         &newer_statistics,
    //         &IndexedAnnotatedFunctions::empty(),
    //         &match_,
    //     )
    //     .unwrap();
    // let mut rows = Vec::new();
    // while let Some(row) = pipeline.next() {
    //     rows.push(row.unwrap().clone().into_owned());
    // }
    // assert_eq!(rows.len(), 7);
    // for row in rows {
    //     for value in row {
    //         print!("{}, ", value);
    //     }
    //     println!()
    // }
}

// We need a way to work around stuff

#[test]
fn test_has_planning_traversal() {
    todo!()
    // let (_tmp_dir, storage) = setup_storage();
    // let mut snapshot = storage.clone().open_snapshot_write();
    // let (type_manager, thing_manager) = load_managers(storage.clone());
    //
    // const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));
    //
    // let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    //
    // let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    // age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    //
    // let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    // name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    //
    // let person_owns_age = person_type
    //     .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered)
    //     .unwrap();
    // person_owns_age.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();
    //
    // let person_owns_name = person_type
    //     .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered)
    //     .unwrap();
    // person_owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, CARDINALITY_ANY).unwrap();
    //
    // let person = [
    //     thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
    //     thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
    //     thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
    // ];
    //
    // let age = [
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap(),
    // ];
    //
    // let name = [
    //     thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("John"))).unwrap(),
    //     thing_manager
    //         .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Alice")))
    //         .unwrap(),
    //     thing_manager
    //         .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Leila")))
    //         .unwrap(),
    // ];
    //
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[1].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[2].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();
    //
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[4].clone()).unwrap();
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    //
    // person[2].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    // person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();
    //
    // let finalise_result = thing_manager.finalise(&mut snapshot);
    // assert!(finalise_result.is_ok());
    // snapshot.commit().unwrap();
    //
    // let mut statistics = Statistics::new(SequenceNumber::new(0));
    // statistics.may_synchronise(&storage).unwrap();
    //
    // // // Executor
    // let query_manager = QueryManager::new();
    // let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    //
    // let snapshot = storage.clone().open_snapshot_read();
    // let query = "match $person isa person, has name $name, has age $age;";
    // let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    // let mut pipeline = query_manager
    //     .prepare_read_pipeline(
    //         snapshot,
    //         thing_manager,
    //         &type_manager,
    //         &function_manager,
    //         &statistics,
    //         &IndexedAnnotatedFunctions::empty(),
    //         &match_,
    //     )
    //     .unwrap();
    // pipeline.initialise().unwrap();
    // let mut rows = Vec::new();
    // while let Some(row) = pipeline.next() {
    //     rows.push(row.unwrap().clone().into_owned());
    // }
    // assert_eq!(rows.len(), 7);
    // for row in rows {
    //     for value in row {
    //         print!("{}, ", value);
    //     }
    //     println!()
    // }
}

#[test]
fn match_delete_has() {
    let (context, thing_manager) = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 10;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let mut insert_pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            thing_manager,
            &context.function_manager,
            &context.statistics,
            &IndexedAnnotatedFunctions::empty(),
            &insert_query,
        )
        .unwrap();
    let (mut iterator, snapshot, thing_manager) = insert_pipeline.into_iterator().unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();
    let thing_manager = Arc::into_inner(thing_manager).unwrap();

    let snapshot = context.storage.clone().open_snapshot_write();
    let delete_query_str = r#"
        match $p isa person, has age $a;
        delete has $a of $p;
    "#;

    let delete_query = typeql::parse_query(delete_query_str).unwrap().into_pipeline();
    let delete_pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            thing_manager,
            &context.function_manager,
            &context.statistics,
            &IndexedAnnotatedFunctions::empty(),
            &delete_query,
        )
        .unwrap();
    let (mut iterator, snapshot, thing_manager) = delete_pipeline.into_iterator().unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();
}
