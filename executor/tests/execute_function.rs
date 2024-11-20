/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::{
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_cache::QueryCache;
use query::query_manager::QueryManager;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils::TempDir;
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

const REACHABILITY_DATA: &str = r#"
insert
        # Chain
        $c1 isa node, has name "c1";
        $c2 isa node, has name "c2";
        $c3 isa node, has name "c3";

        (from: $c1, to: $c2) isa edge;
        (from: $c2, to: $c3) isa edge;

        # Tree
        $t1 isa node, has name "t1";
        $t2 isa node, has name "t2";
        $t3 isa node, has name "t3";
        $t4 isa node, has name "t4";
        $t5 isa node, has name "t5";
        $t6 isa node, has name "t6";
        $t7 isa node, has name "t7";

        (from: $t1, to: $t2) isa edge; (from: $t1, to: $t3) isa edge;
        (from: $t2, to: $t4) isa edge; (from: $t2, to: $t5) isa edge;
        (from: $t3, to: $t6) isa edge; (from: $t3, to: $t7) isa edge;

        # Figure of 8? or of an ant.
        #    (e1)->-.  .-(e3)->-.  .->-(e5)->-.
        #           (e2)        (e4)          (e6)
        #    (e9)-<-'  '-<-(e8)-'  '-<-(e7)-<-'

        $e1 isa node, has name "e1";
        $e2 isa node, has name "e2";
        $e3 isa node, has name "e3";
        $e4 isa node, has name "e4";
        $e5 isa node, has name "e5";
        $e6 isa node, has name "e6";
        $e7 isa node, has name "e7";
        $e8 isa node, has name "e8";
        $e9 isa node, has name "e9";

        (from: $e1, to: $e2) isa edge; (from: $e2, to: $e3) isa edge;
        (from: $e3, to: $e4) isa edge; (from: $e4, to: $e5) isa edge;

        (from: $e5, to: $e6) isa edge; (from: $e6, to: $e7) isa edge;

        (from: $e7, to: $e4) isa edge; (from: $e4, to: $e8) isa edge;
        (from: $e8, to: $e2) isa edge; (from: $e2, to: $e9) isa edge;
"#;

fn setup_common(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    snapshot.commit().unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new(0))));
    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn run_read_query(
    context: &Context,
    query: &str,
) -> Result<(Vec<MaybeOwnedRow<'static>>, HashMap<String, VariablePosition>), Box<PipelineExecutionError>> {
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
    let rows_positions = pipeline.rows_positions().unwrap().clone();
    let (iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    result.map(move |rows| (rows, rows_positions))
}

fn run_write_query(
    context: &Context,
    query: &str,
) -> Result<(Vec<MaybeOwnedRow<'static>>, HashMap<String, VariablePosition>), Box<PipelineExecutionError>> {
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_as_pipeline = typeql::parse_query(query).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query_as_pipeline,
        )
        .unwrap();
    let rows_positions = pipeline.rows_positions().unwrap().clone();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let snapshot = Arc::into_inner(snapshot).unwrap();
    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    snapshot.commit().unwrap();
    result.map(move |rows| (rows, rows_positions))
}

#[test]
fn function_compiles() {
    let context = setup_common(COMMON_SCHEMA);
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1, has age 5;
        $p2 isa person, has name "Bob", has age 2;"#;
    let (rows, _positions) = run_write_query(&context, insert_query_str).unwrap();
    assert_eq!(1, rows.len());

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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
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
        let (rows, _) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2);
    }
}

#[test]
fn function_binary() {
    let context = setup_common(COMMON_SCHEMA);
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1, has age 5;
        $p2 isa person, has name "Bob", has age 2;
        $p3 isa person, has name "Chris", has age 5;
        "#;

    let (rows, _positions) = run_write_query(&context, insert_query_str).unwrap();
    assert_eq!(1, rows.len());

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
        let (rows, _) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2); // Symmetrically Alice & Charlie
    }
}

#[ignore] // TODO: Re-enable when the CONSTANT_CONCEPT_LIMIT assert is fixed
#[test]
fn quadratic_reachability_in_tree() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:from, plays edge:to;
        relation edge, relates from, relates to;
    "#;
    let context = setup_common(custom_schema);

    let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
    assert_eq!(1, rows.len());
    let query_template = r#"
            with
            fun reachable($from: node) -> { node }:
            match
                $return-me has name $name;
                { (from: $from, to: $middle) isa edge; $indirect in reachable($middle); $indirect has name $name; } or
                { (from: $from, to: $direct) isa edge; $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                $from isa node, has name "<<NODE_NAME>>";
                $to in reachable($from);
        "#;
    let placeholder_start_node = "<<NODE_NAME>>";
    {
        // Chain
        let query = query_template.replace(placeholder_start_node, "c1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 2);

        let query = query_template.replace(placeholder_start_node, "c2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1);
    }

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(6, rows.len());

        let query = query_template.replace(placeholder_start_node, "t2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(2, rows.len());
    }

    {
        // ant
        let query = query_template.replace(placeholder_start_node, "e1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(8, rows.len()); // all except e1

        let query = query_template.replace(placeholder_start_node, "e9");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(0, rows.len()); // none

        let query = query_template.replace(placeholder_start_node, "e2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(8, rows.len()); // all except e1. e2 should be reachable from itself
    }
}

#[ignore] // TODO: Re-enable when the CONSTANT_CONCEPT_LIMIT assert is fixed
#[test]
fn linear_reachability_in_tree() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:from, plays edge:to;
        relation edge, relates from, relates to;
    "#;
    let context = setup_common(custom_schema);
    let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
    assert_eq!(1, rows.len());

    let placeholder_start_node = "<<NODE_NAME>>";
    let query_template = r#"
            with
            fun reachable($from: node) -> { node }:
            match
                $return-me has name $name;
                { $middle in reachable($from); (from: $middle, to: $indirect) isa edge; $indirect has name $name; } or
                { (from: $from, to: $direct) isa edge; $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                $from isa node, has name "<<NODE_NAME>>";
                $to in reachable($from);
        "#;

    {
        // Chain
        let query = query_template.replace(placeholder_start_node, "c1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 2);

        let query = query_template.replace(placeholder_start_node, "c2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1);
    }

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(6, rows.len());

        let query = query_template.replace(placeholder_start_node, "t2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(2, rows.len());
    }

    {
        // // ant
        let query = query_template.replace(placeholder_start_node, "e1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(8, rows.len()); // all except e1

        let query = query_template.replace(placeholder_start_node, "e9");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(0, rows.len()); // none

        let query = query_template.replace(placeholder_start_node, "e2");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(8, rows.len()); // all except e1. e2 should be reachable from itself
    }
}

#[test]
fn fibonacci() {
    let custom_schema = r#"define
        attribute number @independent, value long;
    "#;
    let context = setup_common(custom_schema);
    let insert_query = r#"insert
        $n1   1 isa number;
        $n2   2 isa number;
        $n3   3 isa number;
        $n4   4 isa number;
        $n5   5 isa number;
        $n6   6 isa number;
        $n7   7 isa number;
        $n8   8 isa number;
        $n9   9 isa number;
        $n10 10 isa number;
        $n11 11 isa number;
        $n12 12 isa number;
        $n13 13 isa number;
        $n14 14 isa number;
        $n15 15 isa number;
    "#;
    let (rows, _positions) = run_write_query(&context, insert_query).unwrap();
    assert_eq!(1, rows.len());

    {
        let query = r#"
            with
            fun ith_fibonacci_number($i: long) -> { long }:
            match
                $ret isa number;
                { $i == 1; $ret == 1; $ret isa number; } or
                { $i == 2; $ret == 1; $ret isa number; } or
                { $i > 2;
                     $i1 = $i - 1;
                     $i2 = $i - 2;
                     $combined = ith_fibonacci_number($i1) +  ith_fibonacci_number($i2);
                     $ret == $combined;
                     $ret isa number;
                };
            return { $ret };

            match
                $f_7 = ith_fibonacci_number(7);
        "#;
        let (rows, positions) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 1);
        let f_7_position = positions.get("f_7").unwrap().clone();
        assert_eq!(rows[0].get(f_7_position).as_value().clone().unwrap_long(), 13);
    }
}
