/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::sync::Arc;

use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::{
    batch::ImmutableRow,
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI},
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use storage::snapshot::CommittableSnapshot;

use crate::common::{load_managers, setup_storage};

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

fn setup_common() -> (Context, ThingManager ) {
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
    query_manager.execute_schema(&mut snapshot, &type_manager, define).unwrap();
    let seq = snapshot.commit().unwrap();
    let mut statistics = Statistics::new(seq.unwrap());
    statistics.may_synchronise(&storage).unwrap();

    (Context { storage, type_manager, function_manager, query_manager, statistics }, thing_manager)
}

#[test]
fn test_insert() {
    let (context, thing_manager) = setup_common();
    let mut snapshot = context.storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let mut pipeline = context.query_manager
        .prepare_writable_pipeline(
            snapshot,
            thing_manager,
            &context.type_manager,
            &context.function_manager,
            &context.statistics,
            &IndexedAnnotatedFunctions::empty(),
            &query,
        )
        .unwrap();

    assert!(pipeline.next().is_some());
    assert!(pipeline.next().is_none());
    let PipelineContext::Owned(mut snapshot, mut thing_manager) = pipeline.finalise() else { unreachable!() };
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_match_as_pipeline() {
    let (mut context, thing_manager) = setup_common();

    let mut snapshot = context.storage.clone().open_snapshot_write();
    let query_str =  r#"
    insert
        $p0 isa person, has age 10, has age 11, has age 12, has name "John", has name "Alice";
        $p1 isa person, has age 14, has age 13, has  age 10;
        $p2 isa person, has age 13, has name "Leila";

    "#;
    let insert = typeql::parse_query(query_str).unwrap().into_pipeline();
    let mut insert_pipeline = context.query_manager
        .prepare_writable_pipeline(
            snapshot,
            thing_manager,
            &context.type_manager,
            &context.function_manager,
            &context.statistics,
            &IndexedAnnotatedFunctions::empty(),
            &insert,
        )
        .unwrap();
    let mut count = 0;
    while let Some(result) = insert_pipeline.next() {
        assert!(result.is_ok(), "{:?}", result);
        count += 1;
    }
    let PipelineContext::Owned(mut snapshot, mut thing_manager) = insert_pipeline.finalise() else { unreachable!() };
    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    let inserted_seq = snapshot.commit().unwrap();

    let mut newer_statistics = Statistics::new(inserted_seq.unwrap());
    newer_statistics.may_synchronise(&context.storage).unwrap();
    let mut snapshot = context.storage.open_snapshot_write();
    let query = "match $person has $name;";
    // let query = "match $person isa person, has name $name, has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    // // Executor
    let mut pipeline = context.query_manager
        .prepare_writable_pipeline(
            snapshot,
            thing_manager,
            &context.type_manager,
            &context.function_manager,
            &newer_statistics,
            &IndexedAnnotatedFunctions::empty(),
            &match_,
        )
        .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = pipeline.next() {
        rows.push(row.unwrap().clone().into_owned());
    }
    assert_eq!(rows.len(), 7);
    for row in rows {
        for value in row {
            print!("{}, ", value);
        }
        println!()
    }
}
