/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::{
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils::{assert_matches, TempDir};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

const COMMON_SCHEMA: &str = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..), plays membership:member;
        entity organisation plays membership:group;
        relation membership relates member, relates group;
    "#;
fn setup_common(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    snapshot.commit().unwrap();

    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn run_read_query(context: &Context, query: &str) -> Result<Vec<MaybeOwnedRow<'static>>, PipelineExecutionError> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
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

    let (mut iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    let rows: Result<Vec<MaybeOwnedRow<'static>>, PipelineExecutionError> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    rows
}

#[test]
fn function_compiles() {
    let context = setup_common(COMMON_SCHEMA);
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1, has age 5;
        $p2 isa person, has name "Bob", has age 2;"#;
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
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        insert_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
            offset 0;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 3);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
                offset 1;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 1);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
                offset 2;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 0);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
                limit 0;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 0);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
                limit 1;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
                limit 2;
            return {$age_return};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 3);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { age }:
            match
                $p_arg has age $age_return;
            reduce $age_sum = sum($age_return);
            return {$age_sum};

            match
                $p isa person;
                $z in get_ages($p);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2);
    }
}

#[test]
fn function_binary() {
    let context = setup_common(COMMON_SCHEMA);
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1, has age 5;
        $p2 isa person, has name "Bob", has age 2;
        $p3 isa person, has name "Chris", has age 5;
        "#;
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
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        insert_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let query = r#"
            with
            fun same_age_check($p1: person, $p2: person) -> { age }:
            match
                $p1 has age $age1; $p2 has age $age2;
                $age1 == $age2;
            return {$age1};

            match
                $p1 isa person; $p2 isa person;
                $p1 has name $name1; $p2 has name $name2;
                $name1 != $name2;
                $same_age in same_age_check($p1, $p2);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2); // Symmetrically Alice & Charlie
    }
}

#[test]
fn simple_tabled() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:from, plays edge:to;
        relation edge, relates from, relates to;
    "#;
    let context = setup_common(custom_schema);
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $n1 isa node, has name "n1";
        $n2 isa node, has name "n2";
        $n3 isa node, has name "n3";

        (from: $n1, to: $n2) isa edge;
        (from: $n2, to: $n3) isa edge;
    "#;
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
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        insert_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        // quadratic tabling reachability.
        let query = r#"
            with
            fun reachable($from: node) -> { node }:
            match
                $return-me has name $name;
                { (from: $from, to: $middle) isa edge; $indirect in reachable($middle); $indirect has name $name; } or
                { (from: $from, to: $direct) isa edge; $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                $from isa node, has name "n1";
                $to in reachable($from);
        "#;
        let rows = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2);
    }
}
