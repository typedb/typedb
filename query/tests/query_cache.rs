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
use encoding::{graph::definition::definition_key_generator::DefinitionKeyGenerator, value::label::Label};
use function::function_manager::FunctionManager;
use ir::translation::pipeline::TranslatedPipeline;
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
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();

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

fn is_executable_reusable(context: &Context) -> bool {
    let TranslatedPipeline { translated_preamble, translated_given, translated_stages, translated_fetch, .. } =
        context.cache.get_translated(QUERY).unwrap();
    context
        .cache
        .get_executable(
            Arc::new(translated_preamble),
            Arc::new(translated_given),
            Arc::new(translated_stages),
            Arc::new(translated_fetch),
        )
        .is_some()
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
    assert!(context.cache.get_translated(QUERY).is_none());

    prepare(&context);

    // Warm: the identical query string now resolves straight to translated IR.
    assert!(
        context.cache.get_translated(QUERY).is_some(),
        "an identical query string should hit the translation cache"
    );
}

#[test]
fn schema_reset_invalidates_translation_but_keeps_parse_cache() {
    let context = setup();

    prepare(&context);
    assert!(context.cache.get_parsed(QUERY).is_some());
    assert!(context.cache.get_translated(QUERY).is_some());

    // A schema commit can change function resolution, so it flushes the translation cache. Parsing
    // is purely syntactic, so the parse cache must survive.
    context.cache.force_reset(&Statistics::new(SequenceNumber::MIN));
    assert!(context.cache.get_translated(QUERY).is_none(), "a schema reset should invalidate the translation cache");
    assert!(context.cache.get_parsed(QUERY).is_some(), "a schema reset must not invalidate the parse cache");
}

#[test]
fn statistics_updates_do_not_retain_superseded_statistics() {
    let context = setup();

    // A cached plan, as on a server that answered queries before a long import.
    prepare(&context);
    assert!(is_executable_reusable(&context), "the prepared plan should be cached");

    // The import: statistics are replaced after every commit, with no queries in between.
    let superseded = Arc::new(Statistics::new(SequenceNumber::MIN));
    context.cache.set_statistics(superseded.clone());
    let latest = Arc::new(Statistics::new(SequenceNumber::MIN));
    context.cache.set_statistics(latest.clone());
    assert_eq!(Arc::strong_count(&superseded), 1, "superseded statistics should not be retained by the query cache");

    // A lookup checks the plan against the latest statistics, which must not be retained either once superseded.
    assert!(is_executable_reusable(&context), "the plan matches the latest statistics and should still be cached");
    context.cache.set_statistics(Arc::new(Statistics::new(SequenceNumber::MIN)));
    assert_eq!(Arc::strong_count(&latest), 1, "a plan lookup should not retain the statistics it was checked against");
}

#[test]
fn plan_outdated_by_statistics_is_not_reused() {
    let context = setup();

    prepare(&context);
    assert!(is_executable_reusable(&context), "the prepared plan should be cached");

    // The plan was compiled for an empty database: a million people make it outdated.
    let snapshot = context.storage.clone().open_snapshot_read();
    let person = context.type_manager.get_entity_type(&snapshot, &Label::new_static("person")).unwrap().unwrap();
    let mut statistics = Statistics::new(SequenceNumber::MIN);
    statistics.entity_counts.insert(person, 1_000_000);
    context.cache.set_statistics(Arc::new(statistics));
    assert!(!is_executable_reusable(&context), "a plan outdated by the latest statistics should not be reused");

    // An outdated plan is not evicted, only skipped: it is reusable again once the statistics match it.
    context.cache.set_statistics(Arc::new(Statistics::new(SequenceNumber::MIN)));
    assert!(is_executable_reusable(&context), "a plan matching the latest statistics again should be reused");
}
