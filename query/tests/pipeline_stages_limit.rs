/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use function::function_manager::FunctionManager;
use query::{
    error::QueryError,
    query_manager::{QueryManager, MAX_PIPELINE_STAGES},
};
use resource::profile::CommitProfile;
use storage::snapshot::CommittableSnapshot;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn build_pipeline_query(stage_count: usize) -> String {
    let mut query = String::from("match $x isa person;\n");
    for _ in 0..stage_count.saturating_sub(1) {
        query.push_str("select $x;\n");
    }
    query
}

fn setup() -> (
    test_utils::TempDir,
    Arc<storage::MVCCStorage<storage::durability_client::WALClient>>,
    Arc<concept::type_::type_manager::TypeManager>,
    Arc<concept::thing::thing_manager::ThingManager>,
    FunctionManager,
    QueryManager,
) {
    let (tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let schema = "define entity person;";
    let mut snapshot = storage.clone().open_snapshot_schema();
    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, schema_query, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    (tmp_dir, storage, type_manager, thing_manager, function_manager, query_manager)
}

#[test]
fn pipeline_at_limit_is_accepted() {
    let (_tmp_dir, storage, type_manager, thing_manager, function_manager, query_manager) = setup();

    let query_str = build_pipeline_query(MAX_PIPELINE_STAGES);
    let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
    assert_eq!(pipeline.stages.len(), MAX_PIPELINE_STAGES);

    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let result = query_manager.prepare_read_pipeline(
        snapshot,
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &pipeline,
        &query_str,
    );

    if let Err(err) = &result {
        assert!(
            !matches!(err.as_ref(), QueryError::PipelineStagesLimitExceeded { .. }),
            "expected no stages-limit error at the limit, got: {:?}",
            err
        );
    }
}

#[test]
fn pipeline_over_limit_is_rejected() {
    let (_tmp_dir, storage, type_manager, thing_manager, function_manager, query_manager) = setup();

    let over = MAX_PIPELINE_STAGES + 1;
    let query_str = build_pipeline_query(over);
    let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
    assert_eq!(pipeline.stages.len(), over);

    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let result = query_manager.prepare_read_pipeline(
        snapshot,
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &pipeline,
        &query_str,
    );
    let err = match result {
        Ok(_) => panic!("query with too many stages should fail"),
        Err(err) => err,
    };

    match err.as_ref() {
        QueryError::PipelineStagesLimitExceeded { actual, max, .. } => {
            assert_eq!(*actual, over);
            assert_eq!(*max, MAX_PIPELINE_STAGES);
        }
        other => panic!("expected PipelineStagesLimitExceeded, got: {:?}", other),
    }
}
