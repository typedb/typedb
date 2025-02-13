/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::Arc,
};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::{graph::definition::definition_key_generator::DefinitionKeyGenerator, value::value::Value};
use executor::{
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
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
        attribute age value integer;
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

        (start: $c1, end: $c2) isa edge;
        (start: $c2, end: $c3) isa edge;

        # Tree
        $t1 isa node, has name "t1";
        $t2 isa node, has name "t2";
        $t3 isa node, has name "t3";
        $t4 isa node, has name "t4";
        $t5 isa node, has name "t5";
        $t6 isa node, has name "t6";
        $t7 isa node, has name "t7";

        (start: $t1, end: $t2) isa edge; (start: $t1, end: $t3) isa edge;
        (start: $t2, end: $t4) isa edge; (start: $t2, end: $t5) isa edge;
        (start: $t3, end: $t6) isa edge; (start: $t3, end: $t7) isa edge;

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

        (start: $e1, end: $e2) isa edge; (start: $e2, end: $e3) isa edge;
        (start: $e3, end: $e4) isa edge; (start: $e4, end: $e5) isa edge;

        (start: $e5, end: $e6) isa edge; (start: $e6, end: $e7) isa edge;

        (start: $e7, end: $e4) isa edge; (start: $e4, end: $e8) isa edge;
        (start: $e8, end: $e2) isa edge; (start: $e2, end: $e9) isa edge;
"#;

fn setup_common(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, schema)
        .unwrap();
    snapshot.commit().unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
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
            query,
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
            query,
        )
        .unwrap();
    let rows_positions = pipeline.rows_positions().unwrap().clone();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    let snapshot = Arc::into_inner(snapshot).unwrap();
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
                let $z in get_ages($p);
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
                let $z in get_ages($p);
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
                let $z in get_ages($p);
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
                let $z in get_ages($p);
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
                let $z in get_ages($p);
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
                let $z in get_ages($p);
        "#;
        let (rows, _) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 3);
    }

    {
        let query = r#"
            with
            fun get_ages($p_arg: person) -> { integer }:
            match
                $p_arg has age $age_return;
            reduce $age_sum = sum($age_return);
            return {$age_sum};

            match
                $p isa person;
                let $z in get_ages($p);
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
                let $same_age in same_age_check($p1, $p2);
        "#;
        let (rows, _) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 2); // Symmetrically Alice & Charlie
    }
}

#[test]
fn quadratic_reachability_in_tree() {
    // Note: Otherwise fails in cargo because of a stack overflow.
    std::thread::Builder::new().stack_size(32 * 1024 * 1024).name("quadratic_reachability_in_tree".to_owned()).spawn(|| {
        let custom_schema = r#"define
            attribute name value string;
            entity node, owns name @card(0..), plays edge:start, plays edge:end;
            relation edge, relates start, relates end;
        "#;
        let context = setup_common(custom_schema);

        let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
        assert_eq!(1, rows.len());
        let query_template = r#"
                with
                fun reachable($start: node) -> { node }:
                match
                    $return-me has name $name;
                    { edge (start: $start, end: $middle); let $indirect in reachable($middle); $indirect has name $name; } or
                    { edge (start: $start, end: $direct); $direct has name $name; }; # Do we have is yet?
                return { $return-me };

                match
                    $start isa node, has name "<<NODE_NAME>>";
                    let $to in reachable($start);
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
    }).unwrap().join().unwrap();
}

// Note: Fails in cargo because of a stack overflow. Bazel sets a larger stack size
#[test]
fn linear_reachability_in_tree() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:start, plays edge:end;
        relation edge, relates start, relates end;
    "#;
    let context = setup_common(custom_schema);
    let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
    assert_eq!(1, rows.len());

    let placeholder_start_node = "<<NODE_NAME>>";
    let query_template = r#"
            with
            fun reachable($start: node) -> { node }:
            match
                $return-me has name $name;
                { let $middle in reachable($start); edge (start: $middle, end: $indirect); $indirect has name $name; } or
                { edge (start: $start, end: $direct); $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                $start isa node, has name "<<NODE_NAME>>";
                let $to in reachable($start);
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
        attribute number @independent, value integer;
    "#;
    let context = setup_common(custom_schema);
    let insert_query = r#"insert
        $n1  isa number  1;
        $n2  isa number  2;
        $n3  isa number  3;
        $n4  isa number  4;
        $n5  isa number  5;
        $n6  isa number  6;
        $n7  isa number  7;
        $n8  isa number  8;
        $n9  isa number  9;
        $n10 isa number 10;
        $n11 isa number 11;
        $n12 isa number 12;
        $n13 isa number 13;
        $n14 isa number 14;
        $n15 isa number 15;
    "#;
    let (rows, _positions) = run_write_query(&context, insert_query).unwrap();
    assert_eq!(1, rows.len());

    {
        let query = r#"
            with
            fun ith_fibonacci_number($i: integer) -> { number }:
            match
                $ret isa number;
                { $i == 1; $ret == 1; $ret isa number; } or
                { $i == 2; $ret == 1; $ret isa number; } or
                { $i > 2;
                     let $i1 = $i - 1;
                     let $i2 = $i - 2;
                     let $combined = ith_fibonacci_number($i1) +  ith_fibonacci_number($i2);
                     $ret == $combined;
                     $ret isa number;
                };
            return { $ret };

            match
                let $f_7 = ith_fibonacci_number(7);
                let $as_value = $f_7;
        "#;
        let (rows, positions) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 1);
        let answer_position = *positions.get("as_value").unwrap();
        assert_eq!(rows[0].get(answer_position).as_value().clone().unwrap_integer(), 13);
    }
}

#[test]
fn write_pipelines() {
    let context = setup_common(
        r#"
        define
            relation edge relates begin, relates end;
            entity node, plays edge:begin, plays edge:end, owns id @key;
            attribute id, value integer;
            attribute number @independent, value integer;
    "#,
    );
    let insert_query = r#"insert
        $n0  isa number  0;
        $n1  isa number  1;
        $n2  isa number  2;
        $n3  isa number  3;
    "#;
    let (rows, _positions) = run_write_query(&context, insert_query).unwrap();
    assert_eq!(1, rows.len());

    let query = r#"
        with
        fun hops_starting_at($begin: node) -> { integer }:
        match
            $begin isa node;
            let $hops = $hops_inner;
            {
                $edge isa edge, links (begin: $begin, end: $to);
                let $hops_inner_1 = hops_starting_at($to) + 1;
                $hops_inner isa number == $hops_inner_1;
            }
            or {
                not { $no-edge isa edge, links (begin: $begin, end: $anywhere); };
                $hops_inner isa number == 0;
            };
        return { $hops };

        insert
             $x isa node, has id 123456;
        match
            let $hops1 = hops_starting_at($x);
        insert
             $y isa node, has id == $hops1;
             $edge isa edge, links (begin: $x, end: $y);
        match
             let $hops2 = hops_starting_at($x);
         select $hops1, $hops2;
    "#;
    let (rows, positions) = run_write_query(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(*positions.get("hops1").unwrap()), &VariableValue::Value(Value::Integer(0)));
    assert_eq!(rows[0].get(*positions.get("hops2").unwrap()), &VariableValue::Value(Value::Integer(1)));
}
