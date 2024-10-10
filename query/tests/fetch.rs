/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::{pipeline::stage::StageAPI, ExecutionInterrupt};
use function::function_manager::FunctionManager;
use query::query_manager::QueryManager;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn define_schema(storage: Arc<MVCCStorage<WALClient>>, type_manager: &TypeManager, thing_manager: &ThingManager) {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_manager = QueryManager::new();

    let query_str = r#"
    define
    attribute name value string;
    attribute age value long;
    relation friendship relates friend;
    entity person owns name, owns age, plays friendship:friend;
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, schema_query).unwrap();
    snapshot.commit().unwrap();
}

fn insert_data(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_string: &str,
) {
    let mut snapshot = storage.clone().open_snapshot_write();
    let query_manager = QueryManager::new();
    let query = typeql::parse_query(query_string).unwrap().into_pipeline();
    let (pipeline, _) =
        query_manager.prepare_write_pipeline(snapshot, type_manager, thing_manager, function_manager, &query).unwrap();
    let (iterator, context) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let mut snapshot = Arc::into_inner(context.snapshot).unwrap();
    snapshot.commit().unwrap();
}

#[test]
fn fetch() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    define_schema(storage.clone(), type_manager.as_ref(), thing_manager.as_ref());
    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        &function_manager,
        r#"
        insert
          $x isa person, has age 10, has name "Alice";
          $y isa person, has age 11, has name "Bob";
          $z isa person, has age 12, has name "Charlie";
          $p isa person, has age 13, has name "Dixie";
          $q isa person, has age 14, has name "Ellie";
          (friend: $x, friend: $y) isa friendship;
          (friend: $y, friend: $z) isa friendship;
          (friend: $z, friend: $p) isa friendship;
          (friend: $p, friend: $q) isa friendship;
    "#,
    );

    let query = typeql::parse_query(
        r#"
match
    $x isa person, has $a;
    $a isa age;
    $a == 10;
fetch {
    "single attr": $a,
    "single-card attributes": $x.age,
    "single value expression": $a + 1,
    "single answer block": (
        match
        $x has name $name;
        return first $name;
    ),
    "reduce answer block": (
        match
        $x has name $name;
        return count($name);
    ),
    "list positional return block": [
        match
        $x has name $n,
            has age $a;
        return { $n, $a };
    ],
    "list pipeline": [
        match
        $x has name $n,
            has age $a;
        fetch {
            "name": $n
        };
    ],
    "list higher-card attributes": [ $x.name ],
    "list attributes": $x.name[],
    "all attributes": { $x.* }
};"#,
    )
    .unwrap();

    let pipeline = query.into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (executable_pipeline, descriptor) = QueryManager::new()
        .prepare_read_pipeline(snapshot.clone(), &type_manager, thing_manager.clone(), &function_manager, &pipeline)
        .unwrap();
}

//
// #[test]
// fn insert_match_insert_pipeline() {
//     let (_tmp_dir, storage) = setup_storage();
//     let (type_manager, thing_manager, function_manager) = load_managers(storage.clone());
//     let mut statistics = Statistics::new(SequenceNumber::new(0));
//     statistics.may_synchronise(&storage).unwrap();
//
//     define_schema(&storage, &type_manager, &thing_manager);
//     let query_manager = QueryManager::new();
//     let mut snapshot = storage.clone().open_snapshot_write();
//     let query = typeql::parse_query(
//         r#"
//         insert
//             $p1 isa person, has name "John";
//             $p2 isa person, has name "James";
//         match
//             $p_either isa person; $n isa name;
//         insert
//              $p_either has $n;
//     "#,
//     )
//     .unwrap()
//     .into_pipeline();
//     query_manager
//         .prepare_write_pipeline(
//             snapshot,
//             &type_manager,
//             thing_manager,
//             &function_manager,
//             &statistics,
//             &IndexedAnnotatedFunctions::empty(),
//             &query,
//         )
//         .unwrap();
// }
//
// #[test]
// fn insert_insert_pipeline() {
//     let (_tmp_dir, storage) = setup_storage();
//     let (type_manager, thing_manager, function_manager) = load_managers(storage.clone());
//     let mut statistics = Statistics::new(SequenceNumber::new(0));
//     statistics.may_synchronise(&storage).unwrap();
//
//     define_schema(&storage, &type_manager, &thing_manager);
//     let query_manager = QueryManager::new();
//     let mut snapshot = storage.clone().open_snapshot_write();
//     let query = typeql::parse_query(
//         r#"
//         insert
//             $p1 isa person, has name "John";
//             $p2 isa person, has name "James";
//         insert
//             (friend: $p1, friend: $p2) isa friendship;
//     "#,
//     )
//     .unwrap()
//     .into_pipeline();
//     query_manager
//         .prepare_write_pipeline(
//             snapshot,
//             &type_manager,
//             thing_manager,
//             &function_manager,
//             &statistics,
//             &IndexedAnnotatedFunctions::empty(),
//             &query,
//         )
//         .unwrap();
// }
