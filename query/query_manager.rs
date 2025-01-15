/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use compiler::{
    annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline},
    executable::pipeline::{compile_pipeline_and_functions, ExecutablePipeline},
    transformation::transform::apply_transformations,
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    pipeline::Pipeline,
    stage::{ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::{validate_no_cycles, FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    pipeline::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use resource::perf_counters::{QUERY_CACHE_HITS, QUERY_CACHE_MISSES};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use tracing::{event, Level};
use typeql::query::SchemaQuery;

use crate::{define, error::QueryError, query_cache::QueryCache, redefine, undefine};

#[derive(Debug, Clone)]
pub struct QueryManager {
    cache: Option<Arc<QueryCache>>,
}

impl QueryManager {
    pub fn new(cache: Option<Arc<QueryCache>>) -> Self {
        Self { cache }
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        function_manager: &FunctionManager,
        query: SchemaQuery,
    ) -> Result<(), Box<QueryError>> {
        event!(Level::TRACE, "Running schema query:\n{}", query);
        match query {
            SchemaQuery::Define(define) => {
                define::execute(snapshot, type_manager, thing_manager, function_manager, define)
                    .map_err(|err| Box::new(QueryError::Define { typedb_source: err }))
            }
            SchemaQuery::Redefine(redefine) => {
                redefine::execute(snapshot, type_manager, thing_manager, function_manager, redefine)
                    .map_err(|err| Box::new(QueryError::Redefine { typedb_source: err }))
            }
            SchemaQuery::Undefine(undefine) => {
                undefine::execute(snapshot, type_manager, thing_manager, function_manager, undefine)
                    .map_err(|err| Box::new(QueryError::Undefine { typedb_source: err }))
            }
        }
    }

    pub fn prepare_read_pipeline<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, Box<QueryError>> {
        event!(Level::TRACE, "Running read query:\n{}", query);
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = self.translate_pipeline(snapshot.as_ref(), function_manager, query)?;
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
        match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
            Ok(_) => {}
            Err(typedb_source) => return Err(Box::new(QueryError::FunctionDefinition { typedb_source })),
        } // TODO: ^It's not really a retrieval error is it?

        let executable_pipeline = match self
            .cache
            .as_ref()
            .and_then(|cache| cache.get(arced_preamble.clone(), arced_stages.clone(), arced_fetch.clone()))
        {
            Some(executable_pipeline) => {
                QUERY_CACHE_HITS.increment();
                executable_pipeline
            }
            None => {
                // 2: Annotate
                let annotated_schema_functions = function_manager
                    .get_annotated_functions(snapshot.as_ref(), type_manager)
                    .map_err(|err| QueryError::FunctionDefinition { typedb_source: err })?;

                let mut annotated_pipeline = annotate_preamble_and_pipeline(
                    snapshot.as_ref(),
                    type_manager,
                    annotated_schema_functions.clone(),
                    &mut variable_registry,
                    &parameters,
                    (*arced_preamble).clone(),
                    (*arced_stages).clone(),
                    (*arced_fetch).clone(),
                )
                .map_err(|err| QueryError::Annotation { typedb_source: err })?;

                apply_transformations(snapshot.as_ref(), type_manager, &mut annotated_pipeline)
                    .map_err(|err| QueryError::Transformation { typedb_source: err })?;

                let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = annotated_pipeline;
                // 3: Compile
                let executable_pipeline = compile_pipeline_and_functions(
                    thing_manager.statistics(),
                    &variable_registry,
                    &annotated_schema_functions,
                    annotated_preamble,
                    annotated_stages,
                    annotated_fetch,
                    &HashSet::with_capacity(0),
                )
                .map_err(|err| QueryError::ExecutableCompilation { typedb_source: err })?;
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert(arced_preamble, arced_stages, arced_fetch, executable_pipeline.clone())
                }
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch } = executable_pipeline;

        // 4: Executor
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.variable_names(),
            Arc::new(executable_functions),
            &executable_stages,
            executable_fetch,
            Arc::new(parameters),
            None,
        )
        .map_err(|typedb_source| Box::new(QueryError::Pipeline { typedb_source }))
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, Box<QueryError>)> {
        event!(Level::TRACE, "Running write query:\n{}", query);
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters,
        } = match self.translate_pipeline(&snapshot, function_manager, query) {
            Ok(translated) => translated,
            Err(err) => return Err((snapshot, err)),
        };
        let arced_premable = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

        let executable_pipeline = match self
            .cache
            .as_ref()
            .and_then(|cache| cache.get(arced_premable.clone(), arced_stages.clone(), arced_fetch.clone()))
        {
            Some(executable_pipeline) => {
                QUERY_CACHE_HITS.increment();
                executable_pipeline
            }
            None => {
                match validate_no_cycles(&arced_premable.iter().enumerate().collect()) {
                    Ok(_) => {}
                    Err(typedb_source) => {
                        return Err((snapshot, Box::new(QueryError::FunctionDefinition { typedb_source })))
                    }
                } // TODO: ^It's not really a retrieval error is it?

                // 2: Annotate
                let annotated_schema_functions = match function_manager.get_annotated_functions(&snapshot, type_manager)
                {
                    Ok(functions) => functions,
                    Err(err) => {
                        return Err((snapshot, Box::new(QueryError::FunctionDefinition { typedb_source: err })))
                    }
                };

                let mut annotated_pipeline = annotate_preamble_and_pipeline(
                    &snapshot,
                    type_manager,
                    annotated_schema_functions.clone(),
                    &mut variable_registry,
                    &value_parameters,
                    (*arced_premable).clone(),
                    (*arced_stages).clone(),
                    (*arced_fetch).clone(),
                );

                let mut annotated_pipeline = match annotated_pipeline {
                    Ok(annotated_pipeline) => annotated_pipeline,
                    Err(err) => return Err((snapshot, Box::new(QueryError::Annotation { typedb_source: err }))),
                };

                match apply_transformations(&snapshot, type_manager, &mut annotated_pipeline) {
                    Ok(_) => {}
                    Err(err) => return Err((snapshot, Box::new(QueryError::Transformation { typedb_source: err }))),
                };

                let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = annotated_pipeline;

                // 3: Compile
                let executable_pipeline = match compile_pipeline_and_functions(
                    thing_manager.statistics(),
                    &variable_registry,
                    &annotated_schema_functions,
                    annotated_preamble,
                    annotated_stages,
                    annotated_fetch,
                    &HashSet::with_capacity(0),
                ) {
                    Ok(executable) => executable,
                    Err(err) => {
                        return Err((snapshot, Box::new(QueryError::ExecutableCompilation { typedb_source: err })))
                    }
                };
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert(arced_premable, arced_stages, arced_fetch, executable_pipeline.clone())
                }
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch } = executable_pipeline;

        // 4: Executor
        Ok(Pipeline::build_write_pipeline(
            snapshot,
            variable_registry.variable_names(),
            thing_manager,
            executable_stages,
            executable_fetch,
            Arc::new(value_parameters),
        ))
    }

    fn translate_pipeline<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<TranslatedPipeline, Box<QueryError>> {
        let preamble_signatures = HashMapFunctionSignatureIndex::build(
            query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
        );
        let all_function_signatures =
            ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
        translate_pipeline(snapshot, &all_function_signatures, query)
            .map_err(|err| Box::new(QueryError::Representation { typedb_source: err }))
    }
}
