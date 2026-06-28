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
    error::QueryError,
    query_cache::{ConvertedQuery, QueryCache},
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
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, Arc::new(define), SCHEMA)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let cache = Arc::new(QueryCache::new());
    let query_manager = QueryManager::new(Some(cache.clone()));
    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, thing_manager, function_manager, query_manager, cache }
}

fn convert(context: &Context) -> Result<ConvertedQuery, Box<QueryError>> {
    let snapshot = context.storage.clone().open_snapshot_read();
    context.query_manager.convert_query(QUERY, &snapshot, &context.thing_manager, &context.function_manager)
}

#[test]
fn identical_query_string_hits_conversion_cache() {
    let context = setup();

    // Cold: the conversion cache has no entry for this string.
    assert!(context.cache.get_converted(QUERY).is_none());

    // Converting the query parses and translates it, populating the conversion cache.
    assert!(matches!(convert(&context).unwrap(), ConvertedQuery::Data(_)));

    // Warm: the identical query string now resolves straight from the conversion cache.
    assert!(context.cache.get_converted(QUERY).is_some(), "an identical query string should hit the conversion cache");
}

#[test]
fn schema_reset_invalidates_conversion_cache() {
    let context = setup();

    convert(&context).unwrap();
    assert!(context.cache.get_converted(QUERY).is_some());

    // A schema commit flushes the conversion cache, because translation can depend on the schema
    // (resolved function calls). Statistics-only changes must NOT flush it.
    context.cache.force_reset(&Statistics::new(SequenceNumber::MIN));
    assert!(context.cache.get_converted(QUERY).is_none(), "a schema reset should invalidate the conversion cache");
}
