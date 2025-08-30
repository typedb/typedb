/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use compiler::{
    annotation::{
        fetch::{AnnotatedFetch, AnnotatedFetchObject, AnnotatedFetchSome},
        function::{AnnotatedFunctionSignature, FunctionParameterAnnotation},
        pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline, AnnotatedStage},
        type_annotations::{BlockAnnotations, TypeAnnotations},
    },
    executable::pipeline::{compile_pipeline_and_functions, ExecutablePipeline},
    query_structure::{
        extract_pipeline_structure_from, extract_query_structure_from, ParametrisedPipelineStructure,
        PipelineStructure, PipelineStructureAnnotations, QueryStructure, QueryStructureConjunctionID, StructureVariableId,
    },
    transformation::transform::apply_transformations,
    VariablePosition,
};
use concept::{
    error::ConceptReadError,
    thing::thing_manager::ThingManager,
    type_::{attribute_type::AttributeType, type_manager::TypeManager, OwnerAPI, TypeAPI},
};
use encoding::value::value_type::ValueType;
use executor::{
    document::ConceptDocument,
    pipeline::{
        pipeline::Pipeline,
        stage::{ReadPipelineStage, WritePipelineStage},
    },
};
use function::function_manager::{validate_no_cycles, FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    pattern::{ParameterID, Scope, Vertex},
    pipeline::{
        fetch::FetchObject,
        function::Function,
        function_signature::{FunctionID, HashMapFunctionSignatureIndex},
        ParameterRegistry, VariableRegistry,
    },
    translation::pipeline::{TranslatedPipeline, TranslatedStage},
};
use itertools::chain;
use resource::{
    perf_counters::{QUERY_CACHE_HITS, QUERY_CACHE_MISSES},
    profile::{CompileProfile, QueryProfile},
};
use serde::{Deserialize, Serialize};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use tracing::{event, Level};
use typeql::query::SchemaQuery;

use crate::{
    analyse,
    analyse::{AnalysedQuery, FetchStructureAnnotations, FunctionStructureAnnotations, QueryStructureAnnotations},
    define,
    error::QueryError,
    query_cache::QueryCache,
    redefine, undefine,
};

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
        source_query: &str,
    ) -> Result<(), Box<QueryError>> {
        event!(Level::TRACE, "Running schema query:\n{}", query);
        let query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let result = match query {
            SchemaQuery::Define(define) => {
                let profile = query_profile.profile_stage(|| String::from("Define"), 0); // TODO executable id
                let step_profile = profile.extend_or_get(0, || String::from("Define execution"));
                define::execute(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    define,
                    step_profile.storage_counters(),
                )
                .map_err(|err| {
                    Box::new(QueryError::Define { source_query: source_query.to_string(), typedb_source: err })
                })
            }
            SchemaQuery::Redefine(redefine) => {
                let profile = query_profile.profile_stage(|| String::from("Redefine"), 0); // TODO executable id
                let step_profile = profile.extend_or_get(0, || String::from("Redefine execution"));
                redefine::execute(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    redefine,
                    step_profile.storage_counters(),
                )
                .map_err(|err| {
                    Box::new(QueryError::Redefine { source_query: source_query.to_string(), typedb_source: err })
                })
            }
            SchemaQuery::Undefine(undefine) => {
                undefine::execute(snapshot, type_manager, thing_manager, function_manager, undefine).map_err(|err| {
                    Box::new(QueryError::Undefine { source_query: source_query.to_string(), typedb_source: err })
                })
            }
        };

        if query_profile.is_enabled() {
            event!(Level::INFO, "Schema query done.\n{}", query_profile);
        }

        result
    }

    pub fn prepare_read_pipeline<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
        source_query: &str,
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, Box<QueryError>> {
        event!(Level::TRACE, "Running read query:\n{}", query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = translate_pipeline(snapshot.as_ref(), function_manager, query, source_query)?;
        compile_profile.translation_finished();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
        let arced_parameters = Arc::new(parameters);
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
                let executable_pipeline = annotate_and_compile_query(
                    snapshot.as_ref(),
                    source_query,
                    type_manager,
                    thing_manager.clone(),
                    function_manager,
                    compile_profile,
                    &mut variable_registry,
                    arced_parameters.clone(),
                    arced_preamble.clone(),
                    arced_stages.clone(),
                    arced_fetch.clone(),
                )?;
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert(arced_preamble, arced_stages, arced_fetch, executable_pipeline.clone())
                }
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline {
            executable_functions, executable_stages, executable_fetch, pipeline_structure, ..
        } = executable_pipeline;

        // 4: Executor
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.variable_names(),
            pipeline_structure,
            Arc::new(executable_functions),
            &executable_stages,
            executable_fetch,
            arced_parameters,
            None,
            Arc::new(query_profile),
        )
        .map_err(|typedb_source| {
            Box::new(QueryError::Pipeline { source_query: source_query.to_string(), typedb_source })
        })
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
        source_query: &str,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, Box<QueryError>)> {
        event!(Level::TRACE, "Running write query:\n{}", query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters,
        } = match translate_pipeline(&snapshot, function_manager, query, source_query) {
            Ok(translated) => translated,
            Err(err) => return Err((snapshot, err)),
        };
        compile_profile.translation_finished();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
        let arced_parameters = Arc::new(value_parameters);

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
                let executable_pipeline_result = annotate_and_compile_query(
                    &snapshot,
                    source_query,
                    type_manager,
                    thing_manager.clone(),
                    function_manager,
                    compile_profile,
                    &mut variable_registry,
                    arced_parameters.clone(),
                    arced_preamble.clone(),
                    arced_stages.clone(),
                    arced_fetch.clone(),
                );
                match executable_pipeline_result {
                    Ok(executable_pipeline) => {
                        if let Some(cache) = self.cache.as_ref() {
                            cache.insert(arced_preamble, arced_stages, arced_fetch, executable_pipeline.clone())
                        }
                        QUERY_CACHE_MISSES.increment();
                        executable_pipeline
                    }
                    Err(err) => {
                        return Err((snapshot, err));
                    }
                }
            }
        };

        let ExecutablePipeline {
            executable_functions, executable_stages, executable_fetch, pipeline_structure, ..
        } = executable_pipeline;

        // 4: Executor
        Ok(Pipeline::build_write_pipeline(
            snapshot,
            variable_registry.variable_names(),
            pipeline_structure,
            thing_manager,
            Arc::new(executable_functions),
            executable_stages,
            executable_fetch,
            arced_parameters.clone(),
            Arc::new(query_profile),
        ))
    }

    pub fn analyse_query<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        _thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
        source_query: &str,
    ) -> Result<AnalysedQuery, Box<QueryError>> {
        event!(Level::TRACE, "Running analyse query:\n{}", query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = translate_pipeline(snapshot.as_ref(), function_manager, query, source_query)?;
        compile_profile.translation_finished();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

        match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
            Ok(_) => {}
            Err(typedb_source) => {
                return Err(Box::new(QueryError::FunctionDefinition {
                    source_query: source_query.to_string(),
                    typedb_source,
                }))
            }
        }
        compile_profile.validation_finished();

        // 2: Annotate
        let annotated_schema_functions =
            function_manager.get_annotated_functions(snapshot.as_ref(), type_manager).map_err(|err| {
                QueryError::FunctionDefinition { source_query: source_query.to_string(), typedb_source: err }
            })?;

        let annotated_pipeline = annotate_preamble_and_pipeline(
            snapshot.as_ref(),
            type_manager,
            annotated_schema_functions.clone(),
            &mut variable_registry,
            &parameters,
            (*arced_preamble).clone(),
            (*arced_stages).clone(),
            (*arced_fetch).clone(),
        )
        .map_err(|err| QueryError::Annotation { source_query: source_query.to_string(), typedb_source: err })?;
        compile_profile.annotation_finished();

        let arced_parameters = Arc::new(parameters);

        let query_structure = extract_query_structure_from(
            &variable_registry,
            arced_parameters.clone(),
            &annotated_pipeline,
            source_query,
        );
        let query_structure_annotations = QueryStructureAnnotations::build(
            snapshot.as_ref(),
            type_manager,
            arced_parameters,
            source_query,
            &annotated_pipeline,
            &query_structure,
        )
        .map_err(|source| {
            Box::new(QueryError::QueryAnalysisFailed { source_query: source_query.to_owned(), typedb_source: source })
        })?;

        Ok(AnalysedQuery { structure: query_structure, annotations: query_structure_annotations })
    }
}

fn translate_pipeline<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    function_manager: &FunctionManager,
    query: &typeql::query::Pipeline,
    source_query: &str,
) -> Result<TranslatedPipeline, Box<QueryError>> {
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let all_function_signatures =
        ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
    ir::translation::pipeline::translate_pipeline(snapshot, &all_function_signatures, query).map_err(|err| {
        Box::new(QueryError::Representation { source_query: source_query.to_string(), typedb_source: err })
    })
}

fn annotate_and_compile_query(
    snapshot: &impl ReadableSnapshot,
    source_query: &str,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    compile_profile: &mut CompileProfile,
    variable_registry: &mut VariableRegistry,
    arced_parameters: Arc<ParameterRegistry>,
    arced_preamble: Arc<Vec<Function>>,
    arced_stages: Arc<Vec<TranslatedStage>>,
    arced_fetch: Arc<Option<FetchObject>>,
) -> Result<ExecutablePipeline, Box<QueryError>> {
    match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
        Ok(_) => {}
        Err(typedb_source) => {
            return Err(Box::new(QueryError::FunctionDefinition {
                source_query: source_query.to_string(),
                typedb_source,
            }))
        }
    }
    compile_profile.validation_finished();

    // 2: Annotate
    let annotated_schema_functions = match function_manager.get_annotated_functions(snapshot, type_manager) {
        Ok(functions) => functions,
        Err(err) => {
            return Err(Box::new(QueryError::FunctionDefinition {
                source_query: source_query.to_string(),
                typedb_source: err,
            }))
        }
    };

    let annotated_pipeline = annotate_preamble_and_pipeline(
        snapshot,
        type_manager,
        annotated_schema_functions.clone(),
        variable_registry,
        &arced_parameters,
        (*arced_preamble).clone(),
        (*arced_stages).clone(),
        (*arced_fetch).clone(),
    );

    let mut annotated_pipeline = match annotated_pipeline {
        Ok(annotated_pipeline) => annotated_pipeline,
        Err(err) => {
            return Err(Box::new(QueryError::Annotation { source_query: source_query.to_string(), typedb_source: err }))
        }
    };
    compile_profile.annotation_finished();
    // TODO: We can avoid this for the regular query path when we break studio backwards compatibility
    let pipeline_structure =
        extract_pipeline_structure_from(&variable_registry, &annotated_pipeline.annotated_stages, source_query)
            .map(Arc::new);

    match apply_transformations(snapshot, type_manager, &mut annotated_pipeline) {
        Ok(_) => {}
        Err(err) => {
            return Err(Box::new(QueryError::Transformation {
                source_query: source_query.to_string(),
                typedb_source: err,
            }))
        }
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
        pipeline_structure,
    ) {
        Ok(executable) => executable,
        Err(err) => {
            return Err(Box::new(QueryError::ExecutableCompilation {
                source_query: source_query.to_string(),
                typedb_source: err,
            }))
        }
    };
    compile_profile.compilation_finished();
    Ok(executable_pipeline)
}
