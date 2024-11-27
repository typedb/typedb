/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::{
    pipeline::stage::{ExecutionContext, StageIterator},
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils::{assert_matches, TempDir};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const AGE_LABEL: Label = Label::new_static("age");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

fn setup_common() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);
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
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define).unwrap();
    snapshot.commit().unwrap();

    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new(0))));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
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

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
    let attr_age_10 =
        context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
    assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
    snapshot.close_resources()
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
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    while iterator.next().is_some() {}
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    assert_eq!(context.thing_manager.get_relations_in(&snapshot, membership_type).count(), 1);
    snapshot.close_resources()
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
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
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
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
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
    let (iterator, ExecutionContext { .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_match_match() {
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
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "
        match $p isa person;
        match $p has age $a;
    ";
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
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
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
    let (iterator, ExecutionContext { .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_match_delete_has() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 10;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }

    let snapshot = context.storage.clone().open_snapshot_write();
    let delete_query_str = r#"
        match $p isa person, has age $a;
        delete has $a of $p;
    "#;

    let delete_query = typeql::parse_query(delete_query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &delete_query,
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(0, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_insert_match_insert() {
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
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
    insert
        $org isa organisation;
    match
        $p isa person, has age 10;
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
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    while iterator.next().is_some() {}
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    assert_eq!(context.thing_manager.get_relations_in(&snapshot, membership_type).count(), 1);
    snapshot.close_resources()
}

#[test]
fn test_match_sort() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 1, has age 2, has age 3, has age 4;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $age isa age; sort $age desc;";
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
    let named_outputs = pipeline.rows_positions().unwrap().clone();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 4);
    let pos = named_outputs["age"];
    let batch_iter = batch.into_iterator_mut();
    let values = batch_iter
        .map_static(move |res| {
            res.get(pos)
                .as_thing()
                .as_attribute()
                .get_value(&*snapshot, &context.thing_manager)
                .clone()
                .unwrap()
                .unwrap_long()
        })
        .collect::<Vec<_>>();
    assert_eq!([4, 3, 2, 1], values.as_slice());
}

#[test]
fn test_select() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1;
        $p2 isa person, has name "Bob", has age 2;"#;
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age;";
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
        let named_outputs = pipeline.rows_positions().unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(named_outputs.contains_key("p"));
    }
    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age; select $age;";
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
        let named_outputs = pipeline.rows_positions().unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(!named_outputs.contains_key("p"));
    }
}

#[test]
fn test_require() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1;
        $p2 isa person, has name "Bob", has age 2;"#;
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age; require $age;";
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
        let named_outputs = pipeline.rows_positions().unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(named_outputs.contains_key("p"));
    }
}
