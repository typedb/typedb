/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(const_item_mutation, reason = "`&mut CommitProfile::DISABLED` is a dummy")]

use std::{collections::HashMap, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::{
    ExecutionInterrupt,
    batch::Batch,
    pipeline::stage::{ExecutionContext, StageIterator},
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{given_rows::GivenRowsSimple, query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::{TempDir, assert_matches};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const AGE_LABEL: Label = Label::new_static("age");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: Arc<FunctionManager>,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

fn setup_common() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None));
    let query_manager = QueryManager::new(None);
    let schema = r#"
    define
        attribute age value integer;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..), plays membership:member;
        entity organisation plays membership:group;
        relation membership relates member, relates group;
    "#;
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, &define, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

#[test]
fn test_insert() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager,
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
    let attr_age_10 = context
        .thing_manager
        .get_attribute_with_value(&snapshot, age_type, Value::Integer(10), StorageCounters::DISABLED)
        .unwrap()
        .unwrap();
    assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager, StorageCounters::DISABLED).count());
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
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager,
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    while iterator.next().is_some() {}
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    assert_eq!(
        Iterator::count(context.thing_manager.get_relations_in(&snapshot, membership_type, StorageCounters::DISABLED)),
        1
    );
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
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $p isa person;";
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 3);

    let query = "match $person isa person, has name 'John', has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
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
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "
        match $p isa person;
        match $p has age $a;
    ";
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 3);

    let query = "match $person isa person, has name 'John', has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
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
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &insert_query,
            None::<GivenRowsSimple>,
            insert_query_str,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 = context
            .thing_manager
            .get_attribute_with_value(&snapshot, age_type, Value::Integer(10), StorageCounters::DISABLED)
            .unwrap()
            .unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager, StorageCounters::DISABLED).count());
    }

    let snapshot = context.storage.clone().open_snapshot_write();
    let delete_query_str = r#"
        match $p isa person, has age $a;
        delete has $a of $p;
    "#;

    let delete_query = typeql::parse_query(delete_query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &delete_query,
            None::<GivenRowsSimple>,
            delete_query_str,
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 = context
            .thing_manager
            .get_attribute_with_value(&snapshot, age_type, Value::Integer(10), StorageCounters::DISABLED)
            .unwrap()
            .unwrap();
        assert_eq!(0, attr_age_10.get_owners(&snapshot, &context.thing_manager, StorageCounters::DISABLED).count());
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
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
    insert
        $org isa organisation;
    match
        $p isa person, has age 10;
    insert
        (group: $org, member: $p) isa membership;
    "#;

    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    while iterator.next().is_some() {}
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = context.storage.clone().open_snapshot_read();
    let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    assert_eq!(
        Iterator::count(context.thing_manager.get_relations_in(&snapshot, membership_type, StorageCounters::DISABLED)),
        1
    );
}

#[test]
fn test_match_sort() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 1, has age 2, has age 3, has age 4;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &insert_query,
            None::<GivenRowsSimple>,
            insert_query_str,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $age isa age; sort $age desc;";
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
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
                .get_value(&*snapshot, &context.thing_manager, StorageCounters::DISABLED)
                .clone()
                .unwrap()
                .unwrap_integer()
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
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &insert_query,
            None::<GivenRowsSimple>,
            insert_query_str,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age;";
        let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context
            .query_manager
            .prepare_read_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                context.function_manager.clone(),
                &match_,
                None::<GivenRowsSimple>,
                query,
            )
            .unwrap();
        let named_outputs = pipeline.rows_positions().unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(named_outputs.contains_key("p"));
    }
    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age; select $age;";
        let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context
            .query_manager
            .prepare_read_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                context.function_manager.clone(),
                &match_,
                None::<GivenRowsSimple>,
                query,
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
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &insert_query,
            None::<GivenRowsSimple>,
            insert_query_str,
        )
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age; require $age;";
        let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context
            .query_manager
            .prepare_read_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                context.function_manager.clone(),
                &match_,
                None::<GivenRowsSimple>,
                query,
            )
            .unwrap();
        let named_outputs = pipeline.rows_positions().unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(named_outputs.contains_key("p"));
    }
}

fn setup_people() -> Context {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
       insert
       $p isa person, has name 'John', has age 10, has age 11;
       $q isa person, has age 20;
       $r isa person, has name 'Harry';
       $s isa person;
       $o isa organisation;
       membership (member: $p, group: $o);
   "#;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context
}

fn read_rows(context: &Context, query: &str) -> (Batch, HashMap<String, VariablePosition>) {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &match_,
            None::<GivenRowsSimple>,
            query,
        )
        .unwrap();
    let positions = pipeline.rows_positions().unwrap().clone();
    let (iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    (iterator.collect_owned().unwrap(), positions)
}

#[test]
fn test_match_disjunction_branches_binding_the_same_variable() {
    let context = setup_people();
    let (batch, positions) = read_rows(&context, "match $p isa person; { $p has name $v; } or { $p has age $v; };");
    let (person, value) = (positions["p"], positions["v"]);
    assert_eq!(batch.len(), 5);
    assert!(batch.iter().all(|row| !row.get(person).is_none() && !row.get(value).is_none()));
    let mut rows_per_branch: HashMap<u64, usize> = HashMap::new();
    for row in batch.iter() {
        *rows_per_branch.entry(row.provenance().0).or_default() += 1;
    }
    let mut rows_per_branch: Vec<usize> = rows_per_branch.into_values().collect();
    rows_per_branch.sort();
    assert_eq!(rows_per_branch, vec![2, 3]);
}

#[test]
fn test_match_disjunction_branch_with_anonymous_relation() {
    let context = setup_people();
    let (batch, positions) =
        read_rows(&context, "match $p isa person; { membership (member: $p, group: $_); } or { $p has age 20; };");
    let person = positions["p"];
    assert_eq!(batch.len(), 2);
    assert!(batch.iter().all(|row| !row.get(person).is_none()));
    let provenances: Vec<u64> = batch.iter().map(|row| row.provenance().0).collect();
    assert_ne!(provenances[0], provenances[1]);
}

#[test]
fn test_match_disjunction_inside_function() {
    let context = setup_people();
    let (batch, positions) = read_rows(
        &context,
        concat!(
            "with fun score($p: person) -> { integer }: ",
            "match $p has name $n; { $p has age $a; let $s = $a * 2; } or { $n == \"Harry\"; let $s = 0; }; ",
            "return { $s }; ",
            "match $p isa person; let $s in score($p);"
        ),
    );
    let score = positions["s"];
    let mut scores: Vec<i64> = batch
        .iter()
        .map(|row| match row.get(score) {
            VariableValue::Value(Value::Integer(score)) => *score,
            other => panic!("unexpected score {other:?}"),
        })
        .collect();
    scores.sort();
    assert_eq!(scores, vec![0, 20, 22]);
}

#[test]
fn test_match_optional_found_and_not_found() {
    let context = setup_people();
    let (batch, positions) = read_rows(&context, "match $p isa person; try { $p has age $a; };");
    let age = positions["a"];
    assert_eq!(batch.len(), 5);
    let (found, not_found): (Vec<_>, Vec<_>) = batch.iter().partition(|row| !row.get(age).is_none());
    assert_eq!(found.len(), 3);
    assert_eq!(not_found.len(), 2);
    let optional_branch = found[0].provenance().0;
    assert_ne!(optional_branch, 0);
    assert!(found.iter().all(|row| row.provenance().0 == optional_branch));
    assert!(not_found.iter().all(|row| row.provenance().0 == 0));
}
