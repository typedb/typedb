/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use primitive::either::Either;
use compiler::VariablePosition;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use error::UnimplementedFeature;
use executor::{
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use ir::RepresentationError;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use query::error::QueryError;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

fn setup_common(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define).unwrap();
    snapshot.commit().unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new(0))));
    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn run_read_query(
    context: &Context,
    query: &str,
) -> Result<
    (Vec<MaybeOwnedRow<'static>>, HashMap<String, VariablePosition>),
    Either<Box<QueryError>, Box<PipelineExecutionError>>
> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        ).map_err(|query_error| Either::First(query_error))?;
    let rows_positions = pipeline.rows_positions().unwrap().clone();
    let (iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

    result
        .map(move |rows| (rows, rows_positions))
        .map_err(|exec_err| Either::Second(exec_err))
}

fn run_write_query(
    context: &Context,
    query: &str,
) -> Result<(Vec<MaybeOwnedRow<'static>>, HashMap<String, VariablePosition>), Box<PipelineExecutionError>> {
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_as_pipeline = typeql::parse_query(query).unwrap().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query_as_pipeline,
        )
        .unwrap();
    let rows_positions = pipeline.rows_positions().unwrap().clone();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let snapshot = Arc::into_inner(snapshot).unwrap();
    let result: Result<Vec<MaybeOwnedRow<'static>>, Box<PipelineExecutionError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    snapshot.commit().unwrap();
    result.map(move |rows| (rows, rows_positions))
}

#[test]
fn illegal_stages_in_function() {
    let custom_schema = r#"define
        entity person;
    "#;
    let context = setup_common(custom_schema);

    {
        let query = r#"
        with
        fun with_select() -> { integer }:
        match
          let $x = 1;
          let $y = $x + 1;
        select $y;
        return {$y};

        match
            let $two in with_select();
        "#;
        let Either::Second(err) = run_read_query(&context, query).unwrap_err() else { unreachable!(); };
        let err_ref: &PipelineExecutionError = &err;
        match &err_ref {
            PipelineExecutionError::InitialisingMatchIterator { source } => {
                let source_ref: &ConceptReadError = &source;
                assert!(matches!(
                    source_ref,
                    ConceptReadError::UnimplementedFunctionality {
                        functionality: error::UnimplementedFeature::PipelineStageInFunction(_)
                    }
                ))
            }
            _ => Err(err_ref).unwrap(),
        }
    }

    {
        let query = r#"
        with
        fun with_require() -> { integer }:
        match
          let $x = 1;
          let $y = $x + 1;
        require $y;
        return {$y};

        match
            let $two in with_require();
        "#;
        let Either::Second(err) = run_read_query(&context, query).unwrap_err() else { unreachable!() };
        let err_ref: &PipelineExecutionError = &err;
        match &err_ref {
            PipelineExecutionError::InitialisingMatchIterator { source } => {
                let source_ref: &ConceptReadError = &source;
                assert!(matches!(
                    source_ref,
                    ConceptReadError::UnimplementedFunctionality {
                        functionality: error::UnimplementedFeature::PipelineStageInFunction(_)
                    }
                ))
            }
            _ => Err(err_ref).unwrap(),
        }
    }
}


#[test]
fn structs_lists_optionals() {
    let custom_schema = r#"define
        entity person;
        relation friendship, relates person[];
    "#;
    let context = setup_common(custom_schema);

    {
        let query = r#"
        match
            let $z = [1, 2, 3];
        "#;
        run_read_query(&context, query).unwrap_err();
    }

    {
        let query = r#"
        match
            $f isa friendship(person[]: $pl);
        "#;
        let Either::First(err) = run_read_query(&context, query).unwrap_err() else { unreachable!() };
        check_unimplemented_language_feature(&err, error::UnimplementedFeature::Lists);
    }
}

fn check_unimplemented_language_feature(err: &QueryError, expected: UnimplementedFeature) {
    let err_ref: &QueryError = &err;
    match &err_ref {
        QueryError::Representation { typedb_source } => {
            let source_ref: &RepresentationError = &typedb_source;
            assert!(matches!(
                    source_ref,
                    RepresentationError::UnimplementedLanguageFeature {
                        feature: error::UnimplementedFeature::Lists
                        ,..
                    }
                ))
        }
        _ => Err(err_ref).unwrap(),
    }
}


