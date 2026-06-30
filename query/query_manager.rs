/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use compiler::{
    VariablePosition,
    annotation::{
        expression::compiled_expression::ExpressionValueType,
        function::FunctionParameterAnnotation,
        pipeline::{AnnotatedPipeline, annotate_preamble_and_pipeline},
    },
    executable::pipeline::{ExecutablePipeline, ExecutableStage, GivenExecutable, compile_pipeline_and_functions},
    query_structure::{extract_pipeline_structure_from, extract_query_structure_from},
    transformation::transform::apply_transformations,
};
use concept::{
    thing::{ThingAPI, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use executor::{
    batch::Batch,
    pipeline::{
        pipeline::Pipeline,
        stage::{ReadPipelineStage, WritePipelineStage},
    },
};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex, validate_no_cycles};
use ir::{
    LiteralParseError, RepresentationError,
    pattern::{Vertex, variable_category::VariableOptionality},
    pipeline::{
        ParameterRegistry, VariableRegistry,
        fetch::FetchObject,
        function::Function,
        function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    },
    translation::{
        literal::{FromTypeQLLiteral, translate_literal},
        pipeline::{TranslatedGiven, TranslatedPipeline, TranslatedStage},
    },
};
use resource::{
    constants::query::MAX_PIPELINE_STAGES,
    perf_counters::{
        QUERY_CACHE_HITS, QUERY_CACHE_MISSES, QUERY_PARSE_CACHE_HITS, QUERY_PARSE_CACHE_MISSES,
        QUERY_TRANSLATION_CACHE_HITS, QUERY_TRANSLATION_CACHE_MISSES,
    },
    profile::{CompileIRProfile, QueryProfile},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use tracing::{Level, event};
use typeql::query::{QueryStructure, SchemaQuery};

use crate::{
    analyse::{
        AnalysedQuery, FetchStructureAnnotationsFields, FunctionStructureAnnotations, QueryStructureAnnotations,
    },
    define,
    error::QueryError,
    given_rows::{GivenRowDecodeError, GivenRows},
    query_cache::{ParsedQuery, QueryCache},
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

    /// Step 1 of query conversion: parse the raw query string into a typeql AST, consulting the
    /// parse cache. Parsing is schema-independent and needs no transaction, so this can run even
    /// while a write holds the transaction.
    pub fn parse(&self, query: &str) -> Result<ParsedQuery, Box<QueryError>> {
        if let Some(parsed) = self.cache.as_ref().and_then(|cache| cache.get_parsed(query)) {
            QUERY_PARSE_CACHE_HITS.increment();
            return Ok(parsed);
        }
        QUERY_PARSE_CACHE_MISSES.increment();
        let parsed = typeql::parse_query(query)
            .map_err(|err| QueryError::ParseError { source_query: query.to_owned(), typedb_source: err })?;
        let parsed = match parsed.into_structure() {
            QueryStructure::Schema(schema_query) => ParsedQuery::Schema(Arc::new(schema_query)),
            QueryStructure::Pipeline(pipeline) => ParsedQuery::Pipeline(Arc::new(pipeline)),
        };
        if let Some(cache) = self.cache.as_ref() {
            cache.insert_parsed(query, parsed.clone());
        }
        Ok(parsed)
    }

    /// Step 2 of query conversion: translate a parsed data pipeline into IR, consulting the
    /// translation cache. Translation resolves user-defined function calls against the schema, so it
    /// needs a snapshot and is invalidated on schema commits.
    pub fn translate(
        &self,
        query: &str,
        pipeline: &typeql::query::Pipeline,
        snapshot: &impl ReadableSnapshot,
        function_manager: &FunctionManager,
        thing_manager: &ThingManager,
    ) -> Result<TranslatedPipeline, Box<QueryError>> {
        if let Some(translated) = self.cache.as_ref().and_then(|cache| cache.get_translated(query)) {
            QUERY_TRANSLATION_CACHE_HITS.increment();
            return Ok(translated);
        }
        QUERY_TRANSLATION_CACHE_MISSES.increment();
        let translated = translate_pipeline(snapshot, function_manager, pipeline, query)?;
        if let Some(cache) = self.cache.as_ref() {
            cache.may_insert_translated(thing_manager.statistics().sequence_number, query, translated.clone());
        }
        Ok(translated)
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        function_manager: &FunctionManager,
        query: Arc<SchemaQuery>,
        source_query: &str,
    ) -> Result<(), Box<QueryError>> {
        event!(Level::TRACE, "Running schema query:\n{}", query);
        let query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let result = match query.as_ref() {
            SchemaQuery::Define(define) => {
                let profile = query_profile.profile_stage(|| String::from("Define"), 0); // TODO executable id
                let pattern_profile = profile.create_or_get_pattern(|| String::from("Define pattern"));
                let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Define execution"));
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
                let pattern_profile = profile.create_or_get_pattern(|| String::from("Redefine pattern"));
                let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Redefine execution"));
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
        function_manager: Arc<FunctionManager>,
        pipeline: TranslatedPipeline,
        given_rows: Option<impl GivenRows>,
        source_query: &str,
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, Box<QueryError>> {
        event!(Level::TRACE, "Running read query:\n{}", source_query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = pipeline;
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = Arc::new(translated_given);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
        let arced_parameters = Arc::new(parameters);
        let executable_pipeline = match self.cache.as_ref().and_then(|cache| {
            cache.get_compiled(arced_preamble.clone(), arced_given.clone(), arced_stages.clone(), arced_fetch.clone())
        }) {
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
                    &function_manager,
                    compile_profile,
                    &mut variable_registry,
                    arced_parameters.clone(),
                    arced_preamble.clone(),
                    arced_given.clone(),
                    arced_stages.clone(),
                    arced_fetch.clone(),
                )?;
                if let Some(cache) = self.cache.as_ref() {
                    let seq = thing_manager.statistics().sequence_number;
                    cache.may_insert_compiled(
                        seq,
                        arced_preamble,
                        arced_given,
                        arced_stages,
                        arced_fetch,
                        executable_pipeline.clone(),
                    )
                }
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline {
            executable_functions,
            executable_given,
            executable_stages,
            executable_fetch,
            pipeline_structure,
            ..
        } = executable_pipeline;
        let given_batch = validate_and_decode_given(executable_given.clone(), given_rows, &variable_registry)?;

        // 4: Executor
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            function_manager,
            variable_registry.variable_names(),
            (variable_registry.branch_ids_allocated() < 64).then_some(pipeline_structure),
            Arc::new(executable_functions),
            executable_given,
            &executable_stages,
            executable_fetch,
            arced_parameters,
            given_batch,
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
        function_manager: Arc<FunctionManager>,
        pipeline: TranslatedPipeline,
        given_rows: Option<impl GivenRows>,
        source_query: &str,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, Box<QueryError>)> {
        event!(Level::TRACE, "Running write query:\n{}", source_query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters,
        } = pipeline;
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = Arc::new(translated_given);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
        let arced_parameters = Arc::new(value_parameters);

        let executable_pipeline = match self.cache.as_ref().and_then(|cache| {
            cache.get_compiled(arced_preamble.clone(), arced_given.clone(), arced_stages.clone(), arced_fetch.clone())
        }) {
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
                    &function_manager,
                    compile_profile,
                    &mut variable_registry,
                    arced_parameters.clone(),
                    arced_preamble.clone(),
                    arced_given.clone(),
                    arced_stages.clone(),
                    arced_fetch.clone(),
                );
                match executable_pipeline_result {
                    Ok(executable_pipeline) => {
                        if let Some(cache) = self.cache.as_ref() {
                            cache.may_insert_compiled(
                                thing_manager.statistics().sequence_number,
                                arced_preamble,
                                arced_given,
                                arced_stages,
                                arced_fetch,
                                executable_pipeline.clone(),
                            )
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
            executable_functions,
            executable_given,
            executable_stages,
            executable_fetch,
            pipeline_structure,
            ..
        } = executable_pipeline;
        let given_batch = match validate_and_decode_given(executable_given.clone(), given_rows, &variable_registry) {
            Ok(given_rows) => given_rows,
            Err(err) => return Err((snapshot, err)),
        };

        // 4: Executor
        Ok(Pipeline::build_write_pipeline(
            snapshot,
            variable_registry.variable_names(),
            (variable_registry.branch_ids_allocated() < 64).then_some(pipeline_structure),
            thing_manager,
            function_manager,
            Arc::new(executable_functions),
            executable_given,
            executable_stages,
            executable_fetch,
            arced_parameters.clone(),
            given_batch,
            Arc::new(query_profile),
        ))
    }

    pub fn analyse<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        pipeline: TranslatedPipeline,
        source_query: &str,
    ) -> Result<AnalysedQuery, Box<QueryError>> {
        event!(Level::TRACE, "Running analyse query:\n{}", source_query);
        let mut query_profile = QueryProfile::new(tracing::enabled!(Level::TRACE));
        let compile_profile = query_profile.compilation_profile();
        compile_profile.start();
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = pipeline;
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = translated_given.map(Arc::new);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

        match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
            Ok(_) => {}
            Err(typedb_source) => {
                return Err(Box::new(QueryError::FunctionDefinition {
                    source_query: source_query.to_string(),
                    typedb_source,
                }));
            }
        }
        compile_profile.validation_finished();

        // 2: Annotate
        let annotated_schema_functions =
            function_manager.get_annotated_functions(snapshot.as_ref(), type_manager).map_err(|err| {
                QueryError::FunctionDefinition { source_query: source_query.to_string(), typedb_source: *err }
            })?;

        let annotated_pipeline = annotate_preamble_and_pipeline(
            snapshot.as_ref(),
            type_manager,
            annotated_schema_functions.clone(),
            &mut variable_registry,
            &parameters,
            (*arced_preamble).clone(),
            arced_given.map(|given| (*given).clone()),
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
            &variable_registry,
            arced_parameters,
            source_query,
            &annotated_pipeline,
            &query_structure,
        )
        .map_err(|source| {
            Box::new(QueryError::QueryAnalysisFailed { source_query: source_query.to_owned(), typedb_source: source })
        })?;

        Ok(AnalysedQuery {
            source: source_query.to_owned(),
            structure: query_structure,
            annotations: query_structure_annotations,
        })
    }
}

pub fn translate_pipeline<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    function_manager: &FunctionManager,
    query: &typeql::query::Pipeline,
    source_query: &str,
) -> Result<TranslatedPipeline, Box<QueryError>> {
    if query.stages.len() > MAX_PIPELINE_STAGES {
        return Err(Box::new(QueryError::PipelineStagesLimitExceeded {
            source_query: source_query.to_string(),
            actual: query.stages.len(),
            max: MAX_PIPELINE_STAGES,
        }));
    }
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let all_function_signatures =
        ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
    ir::translation::pipeline::translate_pipeline(&all_function_signatures, query).map_err(|err| {
        Box::new(QueryError::Representation { source_query: source_query.to_string(), typedb_source: err })
    })
}

fn annotate_and_compile_query(
    snapshot: &impl ReadableSnapshot,
    source_query: &str,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    compile_profile: &mut CompileIRProfile,
    variable_registry: &mut VariableRegistry,
    arced_parameters: Arc<ParameterRegistry>,
    arced_preamble: Arc<Vec<Function>>,
    arced_given: Arc<Option<TranslatedGiven>>,
    arced_stages: Arc<Vec<TranslatedStage>>,
    arced_fetch: Arc<Option<FetchObject>>,
) -> Result<ExecutablePipeline, Box<QueryError>> {
    if let Err(typedb_source) = validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
        return Err(Box::new(QueryError::FunctionDefinition { source_query: source_query.to_string(), typedb_source }));
    }
    compile_profile.validation_finished();

    // 2: Annotate
    let annotated_schema_functions = match function_manager.get_annotated_functions(snapshot, type_manager) {
        Ok(functions) => functions,
        Err(err) => {
            return Err(Box::new(QueryError::FunctionDefinition {
                source_query: source_query.to_string(),
                typedb_source: *err,
            }));
        }
    };

    let annotated_pipeline = annotate_preamble_and_pipeline(
        snapshot,
        type_manager,
        annotated_schema_functions.clone(),
        variable_registry,
        &arced_parameters,
        (*arced_preamble).clone(),
        (*arced_given).clone(),
        (*arced_stages).clone(),
        (*arced_fetch).clone(),
    );

    let mut annotated_pipeline = match annotated_pipeline {
        Ok(annotated_pipeline) => annotated_pipeline,
        Err(err) => {
            return Err(Box::new(QueryError::Annotation {
                source_query: source_query.to_string(),
                typedb_source: err,
            }));
        }
    };
    compile_profile.annotation_finished();
    // TODO: We can avoid this for the regular query path when we break studio backwards compatibility
    let pipeline_structure = Arc::new(extract_pipeline_structure_from(
        variable_registry,
        annotated_pipeline.annotated_given.as_ref(),
        &annotated_pipeline.annotated_stages,
        source_query,
    ));

    match apply_transformations(snapshot, type_manager, variable_registry, &mut annotated_pipeline) {
        Ok(_) => {}
        Err(err) => {
            return Err(Box::new(QueryError::Transformation {
                source_query: source_query.to_string(),
                typedb_source: err,
            }));
        }
    };

    let AnnotatedPipeline { annotated_preamble, annotated_given, annotated_stages, annotated_fetch } =
        annotated_pipeline;

    // 3: Compile
    let executable_pipeline = match compile_pipeline_and_functions(
        thing_manager.statistics(),
        variable_registry,
        &annotated_schema_functions,
        annotated_preamble,
        annotated_given,
        annotated_stages,
        annotated_fetch,
        pipeline_structure,
    ) {
        Ok(executable) => executable,
        Err(err) => {
            return Err(Box::new(QueryError::ExecutableCompilation {
                source_query: source_query.to_string(),
                typedb_source: err,
            }));
        }
    };
    compile_profile.compilation_finished();
    Ok(executable_pipeline)
}

fn validate_and_decode_given(
    given_executable: Option<Arc<GivenExecutable>>,
    given_rows: Option<impl GivenRows>,
    variable_registry: &VariableRegistry,
) -> Result<Batch, Box<QueryError>> {
    match (given_executable, given_rows) {
        (None, Some(_)) => Err(Box::new(QueryError::UnexpectedGivenRowsProvided {})),
        (Some(_), None) => Err(Box::new(QueryError::NoGivenRowsProvided {})),
        (None, None) => Ok(Batch::new_single_empty_row()),
        (Some(executable), Some(rows)) => {
            let rows_vars: HashSet<&str> = HashSet::from_iter(rows.variables().iter().map(|s| s.as_str()));
            let mut declared_variable_positions = HashMap::with_capacity(rows_vars.len());
            for (i, (var_id, opt)) in executable.variables().iter().zip(executable.optionality().iter()).enumerate() {
                let variable = variable_registry.get_variable_name_or_unnamed(*var_id);
                if *opt == VariableOptionality::Required && !rows_vars.contains(variable) {
                    return Err(Box::new(QueryError::GivenRowsMissingRequiredVariable {
                        variable: variable.to_owned(),
                    }));
                }
                declared_variable_positions.insert(variable, VariablePosition::new(i as u32));
            }
            rows.into_batch_mapped(&declared_variable_positions, executable.expected_types()).map_err(|decode_error| {
                Box::new(QueryError::ErrorDecodingGivenRowEntry { typedb_source: Box::new(decode_error) })
            })
        }
    }
}
