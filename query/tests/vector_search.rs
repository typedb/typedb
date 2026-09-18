/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    thing::{thing_manager::ThingManager, vector_store::VectorStore},
    type_::{TypeAPI, type_manager::TypeManager},
};
use encoding::{
    graph::{Typed, type_::vertex::TypeVertexEncoding},
    value::{label::Label, value::Value},
};
use executor::{ExecutionInterrupt, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{given_rows::GivenRowsSimple, query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: Arc<FunctionManager>,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

const SCHEMA: &str = r#"define
    attribute embedding, value vector(3, "float32");
    entity item owns embedding @card(0..);
"#;

fn setup() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(
        Arc::new(encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator::new()),
        None,
    ));
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, SCHEMA)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn run_write_query(context: &Context, query: &str) -> Vec<MaybeOwnedRow<'static>> {
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_as_pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &query_as_pipeline,
            None::<GivenRowsSimple>,
            query,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let rows: Vec<MaybeOwnedRow<'static>> = iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .collect::<Result<_, _>>()
        .unwrap();
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    rows
}

fn run_read_query(context: &Context, query: &str) -> Vec<MaybeOwnedRow<'static>> {
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
    let (iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    iterator
        .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
        .collect::<Result<_, _>>()
        .unwrap()
}

fn vector_store(context: &Context) -> Arc<VectorStore> {
    context.storage.commit_observer().unwrap().as_any_arc().downcast::<VectorStore>().unwrap()
}

fn embedding_type_id(context: &Context) -> encoding::graph::type_::vertex::TypeID {
    let snapshot = context.storage.clone().open_snapshot_read();
    let attribute_type = context
        .type_manager
        .get_attribute_type(&snapshot, &Label::build("embedding", None))
        .unwrap()
        .unwrap();
    attribute_type.vertex().type_id_()
}

const INSERT: &str = r#"insert
    $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");
    $b isa item, has embedding vector([0.9, 0.1, 0.0], "float32");
    $c isa item, has embedding vector([0.0, 1.0, 0.0], "float32");
"#;

#[test]
fn committed_vectors_live_in_the_store_and_are_searchable() {
    let context = setup();
    run_write_query(&context, INSERT);

    // the commit observer moved the vectors into the vector store
    let store = vector_store(&context);
    assert_eq!(store.indexed_vector_count(embedding_type_id(&context)), 3);

    // ANN search over committed vectors: [1,0,0] and [0.9,0.1,0] are close to [1,0,0]; [0,1,0] is not
    let rows = run_read_query(
        &context,
        r#"match let $emb in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);"#,
    );
    assert_eq!(rows.len(), 2, "expected 2 vectors above similarity 0.9, got: {rows:?}");

    let rows = run_read_query(
        &context,
        r#"match let $emb in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#,
    );
    assert_eq!(rows.len(), 3, "expected all 3 vectors above similarity -1.0, got: {rows:?}");
}

#[test]
fn committed_vector_values_are_read_back_from_the_store() {
    let context = setup();
    run_write_query(&context, INSERT);

    // value read goes: KV key (existence, empty value) -> vector store (the value)
    let snapshot = context.storage.clone().open_snapshot_read();
    let attribute_type = context
        .type_manager
        .get_attribute_type(&snapshot, &Label::build("embedding", None))
        .unwrap()
        .unwrap();
    let mut values: Vec<Vec<f32>> = Vec::new();
    let mut iterator = context
        .thing_manager
        .get_attributes_in(&snapshot, attribute_type, StorageCounters::DISABLED)
        .unwrap();
    while let Some(attribute) = iterator.next() {
        let attribute = attribute.unwrap();
        let value = attribute.get_value(&snapshot, &context.thing_manager, StorageCounters::DISABLED).unwrap();
        let Value::Vector(vector) = value else { panic!("expected vector value") };
        values.push(vector.into_owned());
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(values, vec![vec![0.0, 1.0, 0.0], vec![0.9, 0.1, 0.0], vec![1.0, 0.0, 0.0]]);
}

#[test]
fn uncommitted_vectors_are_found_by_search_in_the_same_transaction() {
    let context = setup();
    // insert and search in ONE write pipeline: the vectors are only in the transaction's buffer,
    // not in the vector store index - the executor merges buffered writes into the candidates
    let rows = run_write_query(
        &context,
        r#"insert
            $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");
            $b isa item, has embedding vector([0.0, 1.0, 0.0], "float32");
        match let $emb in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);"#,
    );
    // the insert stage produces a single row (binding $a and $b); the match stage joins the one
    // buffered vector above the threshold onto it
    assert_eq!(rows.len(), 1, "1 insert row x 1 matching buffered vector, got: {rows:?}");
}

#[test]
fn duplicate_vector_insert_is_deduplicated() {
    let context = setup();
    run_write_query(&context, INSERT);
    // inserting an identical vector must reuse the existing attribute (value-derived identity),
    // exercising the store-backed hash disambiguation against committed (stripped) values
    run_write_query(
        &context,
        r#"insert $d isa item, has embedding vector([1.0, 0.0, 0.0], "float32");"#,
    );
    let store = vector_store(&context);
    assert_eq!(store.indexed_vector_count(embedding_type_id(&context)), 3, "no new vector expected");

    let rows = run_read_query(&context, r#"match $e isa embedding;"#);
    assert_eq!(rows.len(), 3);
    let rows = run_read_query(&context, r#"match $x isa item, has embedding $e;"#);
    assert_eq!(rows.len(), 4);
}
