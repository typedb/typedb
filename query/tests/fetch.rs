/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use answer::{Concept, Thing};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::{
    ExecutionInterrupt,
    document::{DocumentLeaf, DocumentMap, DocumentNode},
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{given_rows::GivenRowsSimple, query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

macro_rules! commit_disabled {
    ($snapshot:expr) => {{
        let mut profile = CommitProfile::DISABLED;
        $snapshot.commit(&mut profile).unwrap();
    }};
}

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
    let schema_query = typeql::parse_query(query_str).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, type_manager, thing_manager, function_manager, schema_query, query_str)
        .unwrap();
    commit_disabled!(snapshot);
}

fn insert_data(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: Arc<FunctionManager>,
    query_string: &str,
) {
    let snapshot = storage.clone().open_snapshot_write();
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    let query = typeql::parse_query(query_string).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            &query,
            None::<GivenRowsSimple>,
            query_string,
        )
        .unwrap();
    let (mut iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    while let Some(row) = iterator.next() {
        row.unwrap();
    }
    let snapshot = Arc::into_inner(context.snapshot).unwrap();
    commit_disabled!(snapshot);
}

#[test]
fn fetch() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None));
    define_schema(storage.clone(), type_manager.as_ref(), thing_manager.as_ref(), &function_manager);
    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        function_manager.clone(),
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
    let query_str = r#"
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
};"#;
    let query = typeql::parse_query(query_str).unwrap();

    let pipeline = query.into_structure().into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_read_pipeline(
            snapshot.clone(),
            &type_manager,
            thing_manager.clone(),
            function_manager,
            &pipeline,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (iterator, _) = pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    for document in iterator {
        println!("{}", document.unwrap());
    }
}

#[test]
fn fetch_ordered_list_attribute_preserves_order() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);

    let query_manager = QueryManager::new(None);
    let schema = r#"
    define
      attribute tag value string;
      entity book owns tag[];
    "#;
    let mut schema_snapshot = storage.clone().open_snapshot_schema();
    query_manager
        .execute_schema(
            &mut schema_snapshot,
            type_manager.as_ref(),
            thing_manager.as_ref(),
            &function_manager,
            typeql::parse_query(schema).unwrap().into_structure().into_schema(),
            schema,
        )
        .unwrap();
    commit_disabled!(schema_snapshot);

    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        &function_manager,
        r#"insert $b isa book, has tag[] ["b", "a"];"#,
    );

    let query_str = r#"
    match $b isa book;
    fetch {
        "tags": $b.tag[]
    };
    "#;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_read_pipeline(
            snapshot.clone(),
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (iterator, _) = pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let documents = iterator.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(documents.len(), 1);

    let DocumentNode::Map(DocumentMap::UserKeys(mut entries)) = documents.into_iter().next().unwrap().root else {
        panic!("expected object document");
    };
    let DocumentNode::List(tags) = entries.drain().next().unwrap().1 else {
        panic!("expected tags to be a list");
    };
    let values = tags
        .list
        .into_iter()
        .map(|node| {
            let DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(attribute)))) = node else {
                panic!("expected attribute leaf in ordered tag list");
            };
            (*attribute
                .get_value(snapshot.as_ref(), &thing_manager, StorageCounters::DISABLED)
                .unwrap()
                .unwrap_string())
            .to_owned()
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![String::from("b"), String::from("a")]);
}

#[test]
fn fetch_ordered_list_attribute_binding_preserves_order() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);

    let query_manager = QueryManager::new(None);
    let schema = r#"
    define
      attribute tag value string;
      entity book owns tag[];
    "#;
    let mut schema_snapshot = storage.clone().open_snapshot_schema();
    query_manager
        .execute_schema(
            &mut schema_snapshot,
            type_manager.as_ref(),
            thing_manager.as_ref(),
            &function_manager,
            typeql::parse_query(schema).unwrap().into_structure().into_schema(),
            schema,
        )
        .unwrap();
    commit_disabled!(schema_snapshot);

    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        &function_manager,
        r#"insert $b isa book, has tag[] ["b", "a"];"#,
    );

    let query_str = r#"
    match $b isa book, has tag[] $tags;
    fetch {
        "tags": $tags
    };
    "#;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_read_pipeline(
            snapshot.clone(),
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &query,
            None::<GivenRowsSimple>,
            query_str,
        )
        .unwrap();

    let (iterator, _) = pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let documents = iterator.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(documents.len(), 1);

    let DocumentNode::Map(DocumentMap::UserKeys(mut entries)) = documents.into_iter().next().unwrap().root else {
        panic!("expected object document");
    };
    let DocumentNode::List(tags) = entries.drain().next().unwrap().1 else {
        panic!("expected tags to be a list");
    };
    let values = tags
        .list
        .into_iter()
        .map(|node| {
            let DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(attribute)))) = node else {
                panic!("expected attribute leaf in ordered tag list");
            };
            (*attribute
                .get_value(snapshot.as_ref(), &thing_manager, StorageCounters::DISABLED)
                .unwrap()
                .unwrap_string())
            .to_owned()
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![String::from("b"), String::from("a")]);
}
