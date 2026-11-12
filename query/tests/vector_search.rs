/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, sync::Arc};

use answer::variable_value::VariableValue;
use concept::{
    index::vector_index::VectorIndex,
    thing::{ThingAPI, thing_manager::ThingManager},
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
    attribute embedding, value vector(3, "float32") @index(hnsw:cosine);
    attribute title, value string;
    entity item owns embedding @card(0..), owns title;
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
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, &define, SCHEMA)
        .unwrap();
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn setup_empty() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(
        Arc::new(encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator::new()),
        None,
    ));
    let query_manager = QueryManager::new(None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn try_define(context: &Context, query: &str) -> Result<(), String> {
    let mut snapshot = context.storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(query).unwrap().into_structure().into_schema();
    context
        .query_manager
        .execute_schema(
            &mut snapshot,
            &context.type_manager,
            &context.thing_manager,
            &context.function_manager,
            &define,
            query,
        )
        .map_err(|err| format!("{err:?}"))?;
    context.type_manager.validate(&snapshot).map_err(|errs| format!("{errs:?}"))?;
    snapshot.commit(&mut CommitProfile::disabled()).map_err(|err| format!("{err:?}"))?;
    Ok(())
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
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    rows
}

fn run_read_query(context: &Context, query: &str) -> Vec<MaybeOwnedRow<'static>> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    run_read_query_on(context, snapshot, query)
}

fn run_read_query_on(
    context: &Context,
    snapshot: Arc<storage::snapshot::ReadSnapshot<WALClient>>,
    query: &str,
) -> Vec<MaybeOwnedRow<'static>> {
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

fn vector_index(context: &Context) -> Arc<VectorIndex> {
    context.thing_manager.vector_index().clone()
}

fn embedding_type_id(context: &Context) -> encoding::graph::type_::vertex::TypeID {
    let snapshot = context.storage.clone().open_snapshot_read();
    let attribute_type =
        context.type_manager.get_attribute_type(&snapshot, &Label::build("embedding", None)).unwrap().unwrap();
    attribute_type.vertex().type_id_()
}

const INSERT: &str = r#"insert
    $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");
    $b isa item, has embedding vector([0.9, 0.1, 0.0], "float32");
    $c isa item, has embedding vector([0.0, 1.0, 0.0], "float32");
"#;

#[test]
fn snapshot_opened_before_commit_does_not_see_committed_vectors() {
    let context = setup();
    let early_snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    run_write_query(&context, INSERT);

    let search =
        r#"match let $e, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#;
    let rows = run_read_query_on(&context, early_snapshot.clone(), search);
    assert_eq!(rows.len(), 0, "snapshot opened before the commit must not see committed vectors, got: {rows:?}");
    let rows = run_read_query_on(&context, early_snapshot, "match $e isa embedding;");
    assert_eq!(rows.len(), 0, "snapshot opened before the commit must not see committed attributes, got: {rows:?}");

    let rows = run_read_query(&context, search);
    assert_eq!(rows.len(), 3, "a fresh snapshot must see all committed vectors, got: {rows:?}");
}

#[test]
fn committed_vectors_live_in_the_store_and_are_searchable() {
    let context = setup();
    run_write_query(&context, INSERT);

    let store = vector_index(&context);
    assert_eq!(store.get_count_for_type(embedding_type_id(&context)), 3);

    let rows = run_read_query(
        &context,
        r#"match let $emb, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);"#,
    );
    assert_eq!(rows.len(), 2, "expected 2 vectors above similarity 0.9, got: {rows:?}");

    let rows = run_read_query(
        &context,
        r#"match let $emb, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#,
    );
    assert_eq!(rows.len(), 3, "expected all 3 vectors above similarity -1.0, got: {rows:?}");
}

#[test]
fn search_by_supertype_covers_subtype_vectors() {
    let context = setup_empty();
    try_define(
        &context,
        r#"define
        attribute embedding @abstract, value vector(3, "float32") @index(hnsw:cosine);
        attribute image-embedding sub embedding;
        attribute text-embedding sub embedding;
        entity item owns image-embedding @card(0..), owns text-embedding @card(0..);
    "#,
    )
    .unwrap();
    run_write_query(
        &context,
        r#"insert
        $a isa item, has image-embedding vector([1.0, 0.0, 0.0], "float32");
        $b isa item, has text-embedding vector([0.9, 0.1, 0.0], "float32");
    "#,
    );

    let rows = run_read_query(
        &context,
        r#"match let $emb, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#,
    );
    assert_eq!(rows.len(), 2, "search by the abstract supertype must cover subtype vectors, got: {rows:?}");

    let rows = run_read_query(
        &context,
        r#"match let $emb, $_ in cosine_similarity_search(image-embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#,
    );
    assert_eq!(rows.len(), 1, "search by a subtype must only cover that subtype, got: {rows:?}");
}

#[test]
fn committed_vector_values_are_read_back_from_the_store() {
    let context = setup();
    run_write_query(&context, INSERT);

    let snapshot = context.storage.clone().open_snapshot_read();
    let attribute_type =
        context.type_manager.get_attribute_type(&snapshot, &Label::build("embedding", None)).unwrap().unwrap();
    let mut values: Vec<Vec<f32>> = Vec::new();
    let mut iterator =
        context.thing_manager.get_attributes_in(&snapshot, attribute_type, StorageCounters::DISABLED).unwrap();
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
    let rows = run_write_query(
        &context,
        r#"insert
            $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");
            $b isa item, has embedding vector([0.0, 1.0, 0.0], "float32");
        match let $emb, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);"#,
    );
    assert_eq!(rows.len(), 1, "1 insert row x 1 matching buffered vector, got: {rows:?}");
}

#[test]
fn duplicate_vector_insert_is_deduplicated() {
    let context = setup();
    run_write_query(&context, INSERT);
    run_write_query(&context, r#"insert $d isa item, has embedding vector([1.0, 0.0, 0.0], "float32");"#);
    let store = vector_index(&context);
    assert_eq!(store.get_count_for_type(embedding_type_id(&context)), 3, "no new vector expected");

    let rows = run_read_query(&context, r#"match $e isa embedding;"#);
    assert_eq!(rows.len(), 3);
    let rows = run_read_query(&context, r#"match $x isa item, has embedding $e;"#);
    assert_eq!(rows.len(), 4);
}

#[test]
fn similarity_scores_are_returned_alongside_elements() {
    let context = setup();
    run_write_query(&context, INSERT);

    let rows = run_read_query(
        &context,
        r#"match let $emb, $sim in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);
           sort $sim desc;"#,
    );
    let scores: Vec<f64> = rows
        .iter()
        .flat_map(|row| {
            row.iter().filter_map(|entry| match entry {
                VariableValue::Value(Value::Double(double)) => Some(*double),
                _ => None,
            })
        })
        .collect();
    let expected = [1.0, 0.9 / (0.81f64 + 0.01).sqrt()];
    assert_eq!(scores.len(), expected.len(), "expected one score per row, got: {rows:?}");
    for (score, expected) in scores.iter().zip(expected) {
        assert!((score - expected).abs() < 1e-5, "expected {expected}, got {score}");
    }
}

#[test]
fn vector_search_requires_two_assigned_variables() {
    let context = setup();
    let query = r#"match let $emb in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.9);"#;
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let result = context.query_manager.prepare_read_pipeline(
        snapshot,
        &context.type_manager,
        context.thing_manager.clone(),
        context.function_manager.clone(),
        &pipeline,
        None::<GivenRowsSimple>,
        query,
    );
    assert!(result.is_err(), "single-variable assignment should be rejected");
}

#[test]
fn vector_literal_in_expression_is_rejected() {
    let context = setup();
    let query = r#"match let $x = vector([1.0, 0.0, 0.0], "float32");"#;
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let result = context.query_manager.prepare_read_pipeline(
        snapshot,
        &context.type_manager,
        context.thing_manager.clone(),
        context.function_manager.clone(),
        &pipeline,
        None::<GivenRowsSimple>,
        query,
    );
    assert!(result.is_err(), "a vector constant in an expression should be a compile error, not a panic");
}

#[test]
fn sort_by_vector_value_is_rejected() {
    let context = setup();
    run_write_query(&context, INSERT);
    let query = r#"match $a isa item, has embedding $emb; sort $emb;"#;
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let result = context.query_manager.prepare_read_pipeline(
        snapshot,
        &context.type_manager,
        context.thing_manager.clone(),
        context.function_manager.clone(),
        &pipeline,
        None::<GivenRowsSimple>,
        query,
    );
    assert!(result.is_err(), "sorting by a vector value should be rejected: vectors have no ordering");
}

fn prepare_read(context: &Context, query: &str) -> Result<(), String> {
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &pipeline,
            None::<GivenRowsSimple>,
            query,
        )
        .map(|_| ())
        .map_err(|err| format!("{err:?}"))
}

#[test]
fn search_requires_a_vector_attribute_type() {
    let context = setup();
    let result = prepare_read(
        &context,
        r#"match let $e, $s in cosine_similarity_search(item, vector([1.0, 0.0, 0.0], "float32"), 0.5);"#,
    );
    assert!(result.is_err(), "searching a non-attribute type must be a query error, not a panic");

    let result = prepare_read(
        &context,
        r#"match let $e, $s in cosine_similarity_search(title, vector([1.0, 0.0, 0.0], "float32"), 0.5);"#,
    );
    assert!(result.is_err(), "searching a non-vector attribute type must be a query error, not empty rows");
}

#[test]
fn search_hides_attributes_left_without_owners() {
    let context = setup();
    run_write_query(&context, INSERT);
    // within one transaction: unlink one embedding, then search; like `isa`, the ownerless
    // attribute must be hidden (embedding is not @independent)
    let rows = run_write_query(
        &context,
        r#"match $p isa item, has embedding $e; $e == vector([1.0, 0.0, 0.0], "float32");
        delete has $e of $p;
        match let $emb, $sim in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);"#,
    );
    assert_eq!(rows.len(), 2, "the ownerless embedding must be hidden from search, got: {rows:?}");
}

#[test]
fn search_output_variables_cannot_be_pre_bound() {
    let context = setup();
    let result = prepare_read(
        &context,
        r#"match
            let $e, $s in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.5);
            let $e2, $s in cosine_similarity_search(embedding, vector([0.0, 1.0, 0.0], "float32"), 0.5);"#,
    );
    assert!(result.is_err(), "two searches assigning the same similarity variable must be rejected");

    let result = prepare_read(
        &context,
        r#"match let $e, $s in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), 0.5);
        match let $e2, $s in cosine_similarity_search(embedding, vector([0.0, 1.0, 0.0], "float32"), 0.5);"#,
    );
    assert!(result.is_err(), "a search output bound by a previous stage must be rejected");
}

#[test]
fn non_finite_vectors_are_rejected_and_dedup_is_by_encoded_bytes() {
    let context = setup();
    let snapshot = context.storage.clone().open_snapshot_read();
    let attribute_type =
        context.type_manager.get_attribute_type(&snapshot, &Label::build("embedding", None)).unwrap().unwrap();
    drop(snapshot);

    // non-finite elements are rejected at write time: cosine similarity is undefined for them
    let mut snapshot = context.storage.clone().open_snapshot_write();
    let nan: Vec<f32> = vec![f32::NAN, 0.0, 0.0];
    let result =
        context.thing_manager.create_attribute(&mut snapshot, attribute_type, Value::Vector(Cow::Borrowed(&nan)));
    assert!(result.is_err(), "a NaN vector must be rejected");
    let inf: Vec<f32> = vec![f32::INFINITY, 0.0, 0.0];
    let result =
        context.thing_manager.create_attribute(&mut snapshot, attribute_type, Value::Vector(Cow::Borrowed(&inf)));
    assert!(result.is_err(), "an infinite vector must be rejected");
    drop(snapshot);

    // deduplication is decided on the encoded bytes: identical bytes resolve to one attribute,
    // while -0.0 and 0.0 (equal as f32, different bytes) stay distinct
    let create = |vector: &Vec<f32>| {
        let mut snapshot = context.storage.clone().open_snapshot_write();
        let attribute = context
            .thing_manager
            .create_attribute(&mut snapshot, attribute_type, Value::Vector(Cow::Borrowed(vector)))
            .unwrap();
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
        attribute.vertex()
    };
    let negative_zero: Vec<f32> = vec![1.0, 0.0, -0.0];
    let positive_zero: Vec<f32> = vec![1.0, 0.0, 0.0];
    let first = create(&negative_zero);
    let second = create(&negative_zero);
    let third = create(&positive_zero);
    assert_eq!(first, second, "identical encoded bytes must resolve to the same attribute ID");
    assert_ne!(first, third, "-0.0 and 0.0 differ in encoded bytes, so they are distinct attributes");
    assert_eq!(vector_index(&context).get_count_for_type(embedding_type_id(&context)), 2);
}

#[test]
fn vector_attribute_requires_index_annotation() {
    let context = setup_empty();
    let result = try_define(&context, r#"define attribute bare-embedding, value vector(3, "float32");"#);
    assert!(result.is_err(), "vector attribute without @index(hnsw:cosine) should be rejected at commit");

    let result = try_define(&context, r#"define attribute name, value string @index(hnsw:cosine);"#);
    assert!(result.is_err(), "@index on a non-vector value type should be rejected");
}

#[test]
fn index_annotation_cannot_be_unset_while_value_type_is_vector() {
    let context = setup();
    let result = try_define(&context, r#"undefine @index from embedding;"#);
    assert!(result.is_err(), "unsetting @index while the value type is still a vector should be rejected");
}

#[test]
fn struct_fields_accept_vector_value_types() {
    use encoding::value::value_type::{ValueType, VectorPrecision, VectorTypeParameters};

    let context = setup_empty();
    let define = r#"define struct doc: content value string, embedding value vector(3, "float32");"#;
    try_define(&context, define).unwrap();

    let embedding_vector_definition = || {
        let snapshot = context.storage.clone().open_snapshot_read();
        let struct_key = context.type_manager.get_struct_definition_key(&snapshot, "doc").unwrap().unwrap();
        let definition = context.type_manager.get_struct_definition(&snapshot, struct_key).unwrap();
        let ValueType::Vector(vector_key) = definition.get_field("embedding").unwrap().value_type.clone() else {
            panic!("expected the embedding field to have a vector value type");
        };
        let parameters = context.type_manager.get_vector_definition(&snapshot, vector_key.clone()).unwrap().parameters;
        (vector_key, parameters)
    };

    let (vector_key, parameters) = embedding_vector_definition();
    assert_eq!(parameters, VectorTypeParameters::new(3, VectorPrecision::Float32));

    try_define(&context, define).expect("re-defining the same struct must be idempotent");
    let (vector_key_after_redefine, _) = embedding_vector_definition();
    assert_eq!(vector_key, vector_key_after_redefine, "unchanged vector field must reuse its definition key");
}
