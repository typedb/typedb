/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compiler::match_::{
    inference::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_inference::infer_types_for_match_block,
    },
    planner::program_plan::ProgramPlan,
};
use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use executor::{
    match_executor::MatchExecutor,
    pipeline::stage::{ExecutionContext, StageAPI},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use ir::{
    program::function_signature::HashMapFunctionSignatureIndex,
    translation::{match_::translate_match, TranslationContext},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::{
    durability_client::WALClient, sequence_number::SequenceNumber, snapshot::CommittableSnapshot, MVCCStorage,
};
use test_utils::assert_matches;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn setup(
    storage: &Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    schema: &str,
    data: &str,
) -> Statistics {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    QueryManager {}.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_write();
    let query = typeql::parse_query(data).unwrap().into_pipeline();
    let (pipeline, _named_outputs) = QueryManager {}
        .prepare_write_pipeline(snapshot, &type_manager, thing_manager.clone(), &FunctionManager::default(), &query)
        .unwrap();
    let (mut iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert_matches!(iterator.next(), Some(Ok(_)));
    assert_matches!(iterator.next(), None);
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(storage).unwrap();
    statistics
}

#[test]
fn test_has_planning_traversal() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let schema = "define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..);
    ";
    let data = "insert
        $_ isa person, has age 10, has age 11, has age 12, has name 'John', has name 'Alice';
        $_ isa person, has age 10, has age 13, has age 14;
        $_ isa person, has age 13, has name 'Leila';
    ";

    let statistics = setup(&storage, type_manager, thing_manager, schema, data);

    let query = "match $person isa person, has name $name, has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
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

    let pattern_plan = compiler::match_::planner::compile(
        &block,
        &HashMap::new(),
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = MatchExecutor::new(&program_plan, &snapshot, &thing_manager, MaybeOwnedRow::empty()).unwrap();

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

    let schema = "define
        entity person owns name @card(0..), plays membership:member;
        relation membership relates member @card(0..);
        attribute name value string;
    ";
    let data = "insert
        $p0 isa person, has name 'John';
        $p1 isa person, has name 'Alice';
        $p2 isa person, has name 'Leila';
        (member: $p0) isa membership;
        (member: $p2) isa membership;
    ";

    let statistics = setup(&storage, type_manager, thing_manager, schema, data);

    let query = "match $person isa person, has name $name; $membership isa membership, links ($person);";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
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

    let pattern_plan = compiler::match_::planner::compile(
        &block,
        &HashMap::new(),
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = MatchExecutor::new(&program_plan, &snapshot, &thing_manager, MaybeOwnedRow::empty()).unwrap();

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

#[test]
fn test_links_intersection() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let schema = "define
        entity user plays purchase:buyer;
        entity order, owns status, owns timestamp, plays purchase:order;
        relation purchase relates buyer, relates order;
        attribute status, value string;
        attribute timestamp, value datetime;
    ";
    let data = "insert
        $u0 isa user; $u1 isa user; $u2 isa user;
        $o0 isa order, has status 'canceled', has timestamp 1970-01-01T00:00;
        $o1 isa order, has status 'dispatched', has timestamp 1970-01-01T00:00;
        $o2 isa order, has status 'paid', has timestamp 1970-01-01T00:00;
        (buyer: $u0, order: $o0) isa purchase;
        (buyer: $u0, order: $o0) isa purchase;
        (buyer: $u1, order: $o1) isa purchase;
    ";

    let statistics = setup(&storage, type_manager, thing_manager, schema, data);

    let query = "match
    $p isa purchase, links (order: $order, buyer: $buyer);
    $order has status $status;
    $order has timestamp $timestamp;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
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

    let pattern_plan = compiler::match_::planner::compile(
        &block,
        &HashMap::new(),
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = MatchExecutor::new(&program_plan, &snapshot, &thing_manager, MaybeOwnedRow::empty()).unwrap();

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

#[test]
fn test_negation_planning_traversal() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let schema = "define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..);
    ";
    let data = "insert
        $_ isa person, has age 10, has age 11, has age 12, has name 'John', has name 'Alice';
        $_ isa person, has age 10, has age 13, has age 14;
        $_ isa person, has age 13, has name 'Leila';
    ";

    let statistics = setup(&storage, type_manager, thing_manager, schema, data);

    let query = "match $person isa person; not { $person has name $name; };";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let mut translation_context = TranslationContext::new();
    let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
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

    let pattern_plan = compiler::match_::planner::compile(
        &block,
        &HashMap::new(),
        &entry_annotations,
        Arc::new(translation_context.variable_registry),
        &HashMap::new(),
        &statistics,
    );
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    let executor = MatchExecutor::new(&program_plan, &snapshot, &thing_manager, MaybeOwnedRow::empty()).unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .into_iter()
        .try_collect::<_, Vec<_>, _>()
        .unwrap();

    for row in &rows {
        for value in row.row() {
            print!("{}, ", value);
        }
        println!()
    }

    assert_eq!(rows.len(), 1);
}
