/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter, sync::Arc};

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
use itertools::Either;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::CommitProfile;
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

        (start: $c1, end_: $c2) isa edge;
        (start: $c2, end_: $c3) isa edge;

        # Tree
        $t1 isa node, has name "t1";
        $t2 isa node, has name "t2";
        $t3 isa node, has name "t3";
        $t4 isa node, has name "t4";
        $t5 isa node, has name "t5";
        $t6 isa node, has name "t6";
        $t7 isa node, has name "t7";

        (start: $t1, end_: $t2) isa edge; (start: $t1, end_: $t3) isa edge;
        (start: $t2, end_: $t4) isa edge; (start: $t2, end_: $t5) isa edge;
        (start: $t3, end_: $t6) isa edge; (start: $t3, end_: $t7) isa edge;

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

        (start: $e1, end_: $e2) isa edge; (start: $e2, end_: $e3) isa edge;
        (start: $e3, end_: $e4) isa edge; (start: $e4, end_: $e5) isa edge;

        (start: $e5, end_: $e6) isa edge; (start: $e6, end_: $e7) isa edge;

        (start: $e7, end_: $e4) isa edge; (start: $e4, end_: $e8) isa edge;
        (start: $e8, end_: $e2) isa edge; (start: $e2, end_: $e9) isa edge;
"#;

fn setup_common(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

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
    let match_ = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
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

    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .into_iter()
        .flat_map(|res| match res {
            Ok(row) => {
                let multiplicity = row.multiplicity() as usize;
                Either::Left(iter::repeat(Ok(row)).take(multiplicity))
            }
            Err(_) => Either::Right(iter::once(res)),
        })
        .collect();

    result.map(move |rows| (rows, rows_positions))
}

fn run_write_query(
    context: &Context,
    query: &str,
) -> Result<(Vec<MaybeOwnedRow<'static>>, HashMap<String, VariablePosition>), Box<PipelineExecutionError>> {
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_as_pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
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
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
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
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .name("quadratic_reachability_in_tree".to_owned())
        .spawn(|| {
            let custom_schema = r#"define
            attribute name value string;
            entity node, owns name @card(0..), plays edge:start, plays edge:end_;
            relation edge, relates start, relates end_;
        "#;
            let context = setup_common(custom_schema);

            let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
            assert_eq!(1, rows.len());
            let query_template = r#"
                with
                fun reachable($start: node) -> { node }:
                match
                    $end isa node;
                    { edge (start: $start, end_: $middle); let $end in reachable($middle); } or
                    { edge (start: $start, end_: $end); };
                return { $end };

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
        })
        .unwrap()
        .join()
        .unwrap();
}

// Note: Fails in cargo because of a stack overflow. Bazel sets a larger stack size
#[test]
fn linear_reachability_in_tree() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:start, plays edge:end_;
        relation edge, relates start, relates end_;
    "#;
    let context = setup_common(custom_schema);
    let (rows, _positions) = run_write_query(&context, REACHABILITY_DATA).unwrap();
    assert_eq!(1, rows.len());

    let placeholder_start_node = "<<NODE_NAME>>";
    let query_template = r#"
            with
            fun reachable($start: node) -> { node }:
            match
                $end isa node;
                { let $middle in reachable($start); edge (start: $middle, end_: $end); } or
                { edge (start: $start, end_: $end); };
            return { $end };

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

        let query = query_template.replace(placeholder_start_node, "e4");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(8, rows.len()); // all except e1
    }
}

#[test]
fn fibonacci() {
    let custom_schema = r#"define
        attribute number @independent, value integer;
    "#;
    let context = setup_common(custom_schema);

    {
        let query = r#"
            with
            fun ith_fibonacci_number($i: integer) -> { integer }:
            match
                let $_ = $ret;
                { $i == 1; let $ret = 1;  } or
                { $i == 2; let $ret = 1; } or
                { $i > 2;
                     let $i1 = $i - 1;
                     let $i2 = $i - 2;
                     let $ret = ith_fibonacci_number($i1) +  ith_fibonacci_number($i2);
                };
            return { $ret };

            match
                let $f_7 = ith_fibonacci_number(7);

        "#;
        let (rows, positions) = run_read_query(&context, query).unwrap();
        assert_eq!(rows.len(), 1);
        let answer_position = *positions.get("f_7").unwrap();
        assert_eq!(rows[0].get(answer_position).as_value().clone().unwrap_integer(), 13);
    }
}

#[test]
fn test_retries_at_negation() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:start, plays edge:end_;
        relation edge, relates start, relates end_;
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
                { let $middle in reachable($start); edge (start: $middle, end_: $indirect); $indirect has name $name; } or
                { edge (start: $start, end_: $direct); $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                let $reachable = "dummy";
                not {
                    $start isa node, has name "<<NODE_NAME>>";
                    $goal isa node, has name "<<GOAL_NAME>>";
                    let $to in reachable($start); $goal is $to;
                };
        "#;

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t2").replace("<<GOAL_NAME>>", "t4");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 0); // reachable, hence not fails
    }

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t2").replace("<<GOAL_NAME>>", "t6");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1); // not reachable
    }

    {
        // ant
        let query = query_template.replace(placeholder_start_node, "e1").replace("<<GOAL_NAME>>", "e4");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 0); // reachable, hence not fails
    }
    {
        // ant
        let query = query_template.replace(placeholder_start_node, "e4").replace("<<GOAL_NAME>>", "e1");
        let (rows, _) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1); // not reachable
    }
}

#[test]
fn test_retries_at_collection() {
    let custom_schema = r#"define
        attribute name value string;
        entity node, owns name @card(0..), plays edge:start, plays edge:end_;
        relation edge, relates start, relates end_;
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
                { let $middle in reachable($start); edge (start: $middle, end_: $indirect); $indirect has name $name; } or
                { edge (start: $start, end_: $direct); $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            match
                $start isa node, has name "<<NODE_NAME>>";
                let $to in reachable($start);
                reduce $reachable_count = count($to);
        "#;

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t1");
        let (rows, positions) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(*positions.get("reachable_count").unwrap()), &VariableValue::Value(Value::Integer(6)));
    }

    {
        // tree
        let query = query_template.replace(placeholder_start_node, "t2");
        let (rows, positions) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(*positions.get("reachable_count").unwrap()), &VariableValue::Value(Value::Integer(2)));
    }
}

#[test]
fn reduce_depends_on_cyclic_function() {
    let custom_schema = r#"define
        attribute name value string;
        attribute weight value integer;
        entity node, owns name @card(0..), plays edge:start, plays edge:end_, owns weight;
        relation edge, relates start, relates end_;
    "#;
    let context = setup_common(custom_schema);
    let data = r#"
    insert
        # Chain
        $c1 isa node, has name "c1", has weight 12;
        $c2 isa node, has name "c2", has weight 34;
        $c3 isa node, has name "c3", has weight 56;

        (start: $c1, end_: $c2) isa edge;
        (start: $c2, end_: $c3) isa edge;

    "#;
    let (rows, _positions) = run_write_query(&context, data).unwrap();
    assert_eq!(1, rows.len());

    let placeholder_start_node = "<<NODE_NAME>>";
    let query_template = r#"
            with
            fun reachable($start: node) -> { node }:
            match
                $return-me has name $name;
                { let $middle in reachable($start); edge (start: $middle, end_: $indirect); $indirect has name $name; } or
                { edge (start: $start, end_: $direct); $direct has name $name; }; # Do we have is yet?
            return { $return-me };

            with
            fun f($start: node) -> integer:
            match
                let $to in reachable($start);
                $to has weight $w;
            return sum($w); # 34 + 56

            match
            $start isa node, has name "<<NODE_NAME>>";
            let $sum = f($start);
        "#;

    {
        // Chain
        let query = query_template.replace(placeholder_start_node, "c1");
        let (rows, positions) = run_read_query(&context, query.as_str()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(*positions.get("sum").unwrap()), &VariableValue::Value(Value::Integer(90)));
    }
}

#[test]
fn write_pipelines() {
    let context = setup_common(
        r#"
        define
            relation edge relates begin, relates end_;
            entity node, plays edge:begin, plays edge:end_, owns id @key;
            attribute id, value integer;
            attribute number @independent, value integer;
    "#,
    );

    let query = r#"
        with
        fun hops_starting_at($begin: node) -> { integer }:
        match
            $begin isa node;
            let $hops = $hops_inner;
            {
                $edge isa edge, links (begin: $begin, end_: $to);
                let $hops_inner = hops_starting_at($to) + 1;
            }
            or {
                not { $no-edge isa edge, links (begin: $begin, end_: $anywhere); };
                let $hops_inner = 0;
            };
        return { $hops };

        insert
             $x isa node, has id 123456;
        match
            let $hops1 = hops_starting_at($x);
        insert
             $y isa node, has id == $hops1;
             $edge isa edge, links (begin: $x, end_: $y);
        match
             let $hops2 = hops_starting_at($x);
         select $hops1, $hops2;
    "#;
    let (rows, positions) = run_write_query(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(*positions.get("hops1").unwrap()), &VariableValue::Value(Value::Integer(0)));
    assert_eq!(rows[0].get(*positions.get("hops2").unwrap()), &VariableValue::Value(Value::Integer(1)));
}
