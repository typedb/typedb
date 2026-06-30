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
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use function::function_manager::FunctionManager;
use query::{
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

fn translate(context: &Context) {
    let snapshot = context.storage.clone().open_snapshot_read();
    let ParsedQuery::Pipeline(pipeline) = context.query_manager.parse(QUERY).unwrap() else {
        panic!("expected a data pipeline");
    };
    context
        .query_manager
        .translate(QUERY, &pipeline, &snapshot, &context.function_manager, &context.thing_manager)
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
    assert!(context.cache.get_translated(QUERY).is_none());

    translate(&context);

    // Warm: the identical query string now resolves straight to translated IR.
    assert!(
        context.cache.get_translated(QUERY).is_some(),
        "an identical query string should hit the translation cache"
    );
}

#[test]
fn schema_reset_invalidates_translation_but_keeps_parse_cache() {
    let context = setup();

    translate(&context);
    assert!(context.cache.get_parsed(QUERY).is_some());
    assert!(context.cache.get_translated(QUERY).is_some());

    // A schema commit can change function resolution, so it flushes the translation cache. Parsing
    // is purely syntactic, so the parse cache must survive.
    context.cache.force_reset(&Statistics::new(SequenceNumber::MIN));
    assert!(context.cache.get_translated(QUERY).is_none(), "a schema reset should invalidate the translation cache");
    assert!(context.cache.get_parsed(QUERY).is_some(), "a schema reset must not invalidate the parse cache");
}
