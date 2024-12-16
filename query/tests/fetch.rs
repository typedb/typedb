/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::ExecutionInterrupt;
use function::function_manager::FunctionManager;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn define_schema(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    function_manager: &FunctionManager,
) {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_manager = QueryManager::new(None);

    let query_str = r#"
    define
      attribute name value string;
      attribute age value integer;
      relation friendship relates friend @card(0..);
      entity person owns name @card(0..), owns age, plays friendship:friend @card(0..);
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, function_manager, schema_query).unwrap();
    snapshot.commit().unwrap();
}

fn insert_data(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_string: &str,
) {
    let snapshot = storage.clone().open_snapshot_write();
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new(0))));
    let query = typeql::parse_query(query_string).unwrap().into_pipeline();
    let pipeline =
        query_manager.prepare_write_pipeline(snapshot, type_manager, thing_manager, function_manager, &query).unwrap();
    let (iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let snapshot = Arc::into_inner(context.snapshot).unwrap();
    snapshot.commit().unwrap();
}

#[test]
fn fetch() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    define_schema(storage.clone(), type_manager.as_ref(), thing_manager.as_ref(), &function_manager);
    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        &function_manager,
        r#"
        insert
          $x isa person, has age 10, has name "Alice", has name "Alicia", has name "Jones";
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

    // TODO: uncomment commented features once they are introduced
    let query = typeql::parse_query(
        r#"
match
    $x isa person, has $a;
    $a isa! $t; $t label age;
    $a == 10;
fetch {
    "single type": $t,
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
        $x has name $name, has age $age;
        return count($name);
    ),
    "list pipeline": [
        match
        $x has name $n,
            has age $a;
        fetch {
            "name": $n
        };
    ],
    "list higher-card attributes": [ $x.name ],
#    "list attributes": $x.name[], # TODO: Uncomment when it's implemented
    "all attributes": { $x.* }
};"#,
    )
    .unwrap();

    let pipeline = query.into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new(0))))
        .prepare_read_pipeline(snapshot.clone(), &type_manager, thing_manager.clone(), &function_manager, &pipeline)
        .unwrap();

    let (iterator, _) = pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    for document in iterator {
        println!("{}", document.unwrap());
    }
}
