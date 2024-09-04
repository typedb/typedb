/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    thing::{thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::pipeline::{StageAPI, StageIterator};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::{durability_client::WALClient, MVCCStorage, snapshot::CommittableSnapshot};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
}

fn setup_common() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone());
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();
    let schema = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..), plays membership:member;
        entity organisation plays membership:group;
        relation membership relates member, relates group;
    "#;
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    let seq = snapshot.commit().unwrap();


    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone());
    Context { storage, type_manager, function_manager, query_manager, thing_manager }
}

#[test]
fn test_insert() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
        )
        .unwrap();
    let (mut iterator, snapshot) = pipeline.into_iterator().unwrap();
    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 = context
            .thing_manager
            .get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_insert_insert() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
    insert
        $p isa person, has age 10;
        $org isa organisation;
    insert
        (group: $org, member: $p) isa membership;
    "#;
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
        );
    if let Err((_, err)) = pipeline {
        dbg!(err);
    }
    //
    // let (mut iterator, snapshot) = pipeline.into_iterator().unwrap();
    // let row = iterator.next();
    // assert!(matches!(&row, &Some(Ok(_))));
    // assert_eq!(row.unwrap().unwrap().len(), 3);
    // assert!(matches!(iterator.next(), None));
    // let snapshot = Arc::into_inner(snapshot).unwrap();
    // snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
        assert_eq!(context.thing_manager.get_relations_in(&snapshot, membership_type).count(), 1);
        snapshot.close_resources()
    }
}


#[test]
fn test_match() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
       insert
       $p isa person, has age 10, has name 'John';
       $q isa person, has age 20, has name 'Alice';
       $r isa person, has age 30, has name 'Harry';
   "#;
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
        )
        .unwrap();
    let (mut iterator, snapshot) = pipeline.into_iterator().unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $p isa person;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        )
        .unwrap();
    let (iterator, snapshot) = pipeline.into_iterator().unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 3);

    let query = "match $person isa person, has name 'John', has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        )
        .unwrap();
    let (iterator, snapshot) = pipeline.into_iterator().unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 1);
    let snapshot = Arc::into_inner(snapshot);
}

#[test]
fn test_match_as_pipeline() {
    todo!("This hits a todo");
}

#[test]
fn match_delete_has() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 10;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let insert_pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = insert_pipeline.into_iterator().unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

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
            context.thing_manager.clone(),
            &context.function_manager,
            &delete_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = delete_pipeline.into_iterator().unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();
}
