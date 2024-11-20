/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use compiler::{
    annotation::pipeline::{annotate_pipeline, AnnotatedPipeline},
    executable::pipeline::{compile_pipeline, ExecutablePipeline},
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    pipeline::Pipeline,
    stage::{ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    pipeline::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;
use compiler::executable::pipeline::ExecutableStage;
use resource::perf_counters::{QUERY_CACHE_HITS, QUERY_CACHE_MISSES};

use crate::{define, error::QueryError, redefine, undefine};
use crate::query_cache::QueryCache;

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
        query: SchemaQuery,
    ) -> Result<(), QueryError> {
        match query {
            SchemaQuery::Define(define) => define::execute(snapshot, type_manager, thing_manager, define)
                .map_err(|err| QueryError::Define { typedb_source: err }),
            SchemaQuery::Redefine(redefine) => redefine::execute(snapshot, type_manager, thing_manager, redefine)
                .map_err(|err| QueryError::Redefine { typedb_source: err }),
            SchemaQuery::Undefine(undefine) => undefine::execute(snapshot, type_manager, thing_manager, undefine)
                .map_err(|err| QueryError::Undefine { typedb_source: err }),
        }
    }

    pub fn prepare_read_pipeline<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, QueryError> {
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = self.translate_pipeline(snapshot.as_ref(), function_manager, query)?;
        let arced_premable = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch= Arc::new(translated_fetch);

        let executable_pipeline = match self.cache.as_ref().map(|cache| 
            cache.get(arced_premable.clone(), arced_stages.clone(), arced_fetch.clone())
        ).flatten() {
            Some(executable_pipeline) => {
                QUERY_CACHE_HITS.increment();
                executable_pipeline
            },
            None => {
                // 2: Annotate
                let annotated_schema_functions = function_manager
                    .get_annotated_functions(snapshot.as_ref(), type_manager)
                    .map_err(|err| QueryError::FunctionRetrieval { typedb_source: err })?;

                let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = annotate_pipeline(
                    snapshot.as_ref(),
                    type_manager,
                    &annotated_schema_functions,
                    &mut variable_registry,
                    &parameters,
                    (*arced_premable).clone(),
                    (*arced_stages).clone(),
                    (*arced_fetch).clone(),
                )
                    .map_err(|err| QueryError::Annotation { typedb_source: err })?;

                // 3: Compile
                let executable_pipeline = compile_pipeline(
                    thing_manager.statistics(),
                    &variable_registry,
                    &annotated_schema_functions,
                    annotated_preamble,
                    annotated_stages,
                    annotated_fetch,
                    &HashSet::with_capacity(0),
                    false,
                )
                    .map_err(|err| QueryError::ExecutableCompilation { typedb_source: err })?;
                self.cache.as_ref().map(|cache| cache.insert(arced_premable, arced_stages, arced_fetch, executable_pipeline.clone()));
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
        .map_err(|typedb_source| QueryError::Pipeline { typedb_source })
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, QueryError)> {
        let is_stocking = format!("{query}").contains("STOCKING");

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
        let arced_fetch= Arc::new(translated_fetch);

        // let executable_pipeline = match self.cache.as_ref().map(|cache|
        //     cache.get(arced_premable.clone(), arced_stages.clone(), arced_fetch.clone())
        // ).flatten() {
        //     Some(executable_pipeline) => {
        //         QUERY_CACHE_HITS.increment();
        //         executable_pipeline
        //     },
        //     None => {
                // 2: Annotate
                let annotated_schema_functions = match function_manager.get_annotated_functions(&snapshot, type_manager) {
                    Ok(functions) => functions,
                    Err(err) => return Err((snapshot, QueryError::FunctionRetrieval { typedb_source: err })),
                };

                let annotated_pipeline = annotate_pipeline(
                    &snapshot,
                    type_manager,
                    &annotated_schema_functions,
                    &mut variable_registry,
                    &value_parameters,
                    (*arced_premable).clone(),
                    (*arced_stages).clone(),
                    (*arced_fetch).clone(),
                );

                let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = match annotated_pipeline {
                    Ok(annotated_pipeline) => annotated_pipeline,
                    Err(err) => return Err((snapshot, QueryError::Annotation { typedb_source: err })),
                };

                // 3: Compile
                let executable_pipeline = match compile_pipeline(
                    thing_manager.statistics(),
                    &variable_registry,
                    &annotated_schema_functions,
                    annotated_preamble,
                    annotated_stages,
                    annotated_fetch,
                    &HashSet::with_capacity(0),
                    is_stocking,
                ) {
                    Ok(executable) => executable,
                    Err(err) => return Err((snapshot, QueryError::ExecutableCompilation { typedb_source: err })),
                };
                // self.cache.as_ref().map(|cache| cache.insert(arced_premable, arced_stages, arced_fetch, executable_pipeline.clone()));
                // QUERY_CACHE_MISSES.increment();
                // executable_pipeline
            // }
        // };

        if is_stocking {
            match executable_pipeline.executable_stages.first().unwrap() {
                ExecutableStage::Match(match_) => {
                    println!("Running query:");
                    println!("{}", query);
                    println!("{}", match_)
                },
                _ => {}
            }
        }

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
    ) -> Result<TranslatedPipeline, QueryError> {
        let preamble_signatures = HashMapFunctionSignatureIndex::build(
            query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
        );
        let all_function_signatures =
            ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
        translate_pipeline(snapshot, &all_function_signatures, query)
            .map_err(|err| QueryError::Representation { typedb_source: err })
    }
}
