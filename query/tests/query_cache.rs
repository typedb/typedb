/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
};
use function::function_manager::FunctionManager;
use query::{
    given_rows::GivenRowsSimple,
    query_cache::{ParsedQuery, QueryCache},
    query_manager::QueryManager,
};
use resource::profile::CommitProfile;
use storage::{
    MVCCStorage, durability_client::WALClient, sequence_number::SequenceNumber, snapshot::CommittableSnapshot,
};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const SCHEMA: &str = r#"define
    attribute name, value string;
    entity person, owns name;
"#;
const QUERY: &str = "match $p isa person, has name $n;";

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: Arc<FunctionManager>,
    query_manager: QueryManager,
    cache: Arc<QueryCache>,
    _tmp_dir: TempDir,
}

fn setup() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None));

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    QueryManager::new(None)
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, &define, SCHEMA)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let cache = Arc::new(QueryCache::new());
    let query_manager = QueryManager::new(Some(cache.clone()));
    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, thing_manager, function_manager, query_manager, cache }
}

fn prepare(context: &Context) {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let ParsedQuery::Pipeline(pipeline) = context.query_manager.parse(QUERY).unwrap() else {
        panic!("expected a data pipeline");
    };
    context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &pipeline,
            None::<GivenRowsSimple>,
            QUERY,
        )
        .unwrap();
}

#[test]
fn identical_query_string_hits_parse_cache() {
    let context = setup();

    // Cold: the parse cache has no entry for this string.
    assert!(context.cache.get_parsed(QUERY).is_none());

    assert!(matches!(context.query_manager.parse(QUERY).unwrap(), ParsedQuery::Pipeline(_)));

    // Warm: the identical query string now resolves straight from the parse cache.
    assert!(context.cache.get_parsed(QUERY).is_some(), "an identical query string should hit the parse cache");
}

#[test]
fn identical_query_string_hits_translation_cache() {
    let context = setup();

    // Cold: the translation cache has no entry for this string.
    assert!(context.cache.get_translated(context.thing_manager.statistics().sequence_number, QUERY).is_none());

    prepare(&context);

    // Warm: the identical query string now resolves straight to translated IR.
    assert!(
        context.cache.get_translated(context.thing_manager.statistics().sequence_number, QUERY).is_some(),
        "an identical query string should hit the translation cache"
    );
}

#[test]
fn schema_reset_invalidates_translation_but_keeps_parse_cache() {
    let context = setup();

    prepare(&context);
    assert!(context.cache.get_parsed(QUERY).is_some());
    assert!(context.cache.get_translated(context.thing_manager.statistics().sequence_number, QUERY).is_some());

    // A schema commit can change function resolution, so it flushes the translation cache. Parsing
    // is purely syntactic, so the parse cache must survive.
    context.cache.force_reset(&Statistics::new(SequenceNumber::MIN));
    assert!(
        context.cache.get_translated(context.thing_manager.statistics().sequence_number, QUERY).is_none(),
        "a schema reset should invalidate the translation cache"
    );
    assert!(context.cache.get_parsed(QUERY).is_some(), "a schema reset must not invalidate the parse cache");
}

#[test]
fn newer_schema_translation_is_not_visible_to_older_reader() {
    let context = setup();
    let old_snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let schema = "define fun f() -> integer: match let $x = 1; return first $x;";
    let mut snapshot = context.storage.clone().open_snapshot_schema();
    context
        .query_manager
        .execute_schema(
            &mut snapshot,
            &context.type_manager,
            &context.thing_manager,
            &context.function_manager,
            &typeql::parse_query(schema).unwrap().into_structure().into_schema(),
            schema,
        )
        .unwrap();
    let mut profile = CommitProfile::DISABLED;
    let schema_commit = snapshot.commit(&mut profile).unwrap().unwrap();
    context.cache.force_reset(&Statistics::new(schema_commit));
    let (new_types, _) = load_managers(context.storage.clone(), None);
    let new_things = Arc::new(ThingManager::new(
        Arc::new(ThingVertexGenerator::load(context.storage.clone()).unwrap()),
        new_types.clone(),
        Arc::new(Statistics::new(schema_commit)),
    ));
    let source = "match let $x = f();";
    let pipeline = typeql::parse_query(source).unwrap().into_structure().into_pipeline();
    context
        .query_manager
        .prepare_read_pipeline(
            Arc::new(context.storage.clone().open_snapshot_read()),
            &new_types,
            new_things,
            context.function_manager.clone(),
            &pipeline,
            None::<GivenRowsSimple>,
            source,
        )
        .unwrap();
    assert!(
        context.cache.get_translated(schema_commit, source).is_some(),
        "new snapshot must warm the translation cache"
    );
    let uncached = QueryManager::new(None).prepare_read_pipeline(
        old_snapshot.clone(),
        &context.type_manager,
        context.thing_manager.clone(),
        context.function_manager.clone(),
        &pipeline,
        None::<GivenRowsSimple>,
        source,
    );
    assert!(uncached.is_err(), "old schema must not resolve the newly defined function");
    let cached = context.query_manager.prepare_read_pipeline(
        old_snapshot,
        &context.type_manager,
        context.thing_manager.clone(),
        context.function_manager.clone(),
        &pipeline,
        None::<GivenRowsSimple>,
        source,
    );
    assert!(cached.is_err(), "cached translation leaked a newer schema into the old reader");
    assert!(
        context.cache.get_translated(schema_commit, source).is_some(),
        "old reader must not evict the current schema translation"
    );
}
