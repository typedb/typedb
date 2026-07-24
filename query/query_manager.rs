/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    analyse::{AnalysedQuery, QueryStructureAnnotations},
    define,
    error::QueryError,
    given_rows::GivenRows,
    query_cache::QueryCache,
    redefine, undefine,
};
use compiler::{
    annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline},
    executable::pipeline::{compile_pipeline_and_functions, CompiledPipeline, ExecutableStage, GivenExecutable},
    query_structure::{extract_pipeline_structure_from, extract_query_structure_from},
    transformation::transform::apply_transformations,
    VariablePosition,
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::stage::ExecutionContext;
use executor::{
    batch::Batch,
    pipeline::{
        pipeline::Pipeline,
        stage::{ReadPipelineStage, StageAPI, WritePipelineStage},
    },
};
use function::function_manager::{validate_no_cycles, FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::pipeline::QueryContext;
use ir::{
    pattern::variable_category::VariableOptionality,
    pipeline::{
        fetch::FetchObject, function::Function,
        function_signature::{FunctionID, HashMapFunctionSignatureIndex},
        ParameterRegistry,
        VariableRegistry,
    },
    translation::{
        literal::FromTypeQLLiteral,
        pipeline::{TranslatedGiven, TranslatedPipeline, TranslatedStage},
    },
};
use resource::{
    constants::query::MAX_PIPELINE_STAGES,
    perf_counters::{
        QUERY_CACHE_HITS, QUERY_CACHE_MISSES, QUERY_PARSE_CACHE_HITS, QUERY_PARSE_CACHE_MISSES,
        QUERY_TRANSLATION_CACHE_HITS, QUERY_TRANSLATION_CACHE_MISSES,
    },
    profile::QueryProfile,
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use tracing::{event, Level};
use typeql::query::{QueryStructure, SchemaQuery};

#[derive(Debug, Clone)]
pub struct QueryManager {
    cache: Option<Arc<QueryCache>>,
}
//
// #[derive(Debug)]
// pub struct QueryContext {
//     source_query: String,
//     profile: Arc<QueryProfile>,
// }
//
// impl QueryContext {
//     pub fn new(source_query: String) -> Self {
//         Self { source_query, profile: Arc::new(QueryProfile::new(tracing::enabled!(Level::TRACE))) }
//     }
//
//     pub fn new_profile_disabled(source_query: String) -> Self {
//         Self { source_query, profile: Arc::new(QueryProfile::new(false)) }
//     }
//
//     pub fn new_profile_enabled(source_query: String) -> Self {
//         Self { source_query, profile: Arc::new(QueryProfile::new(true)) }
//     }
//
//     pub fn source_query(&self) -> &str {
//         &self.source_query
//     }
// }

#[derive(Debug)]
pub enum ParsedQuery {
    Schema(ParsedSchemaQuery),
    Pipeline(ParsedPipeline),
}

impl ParsedQuery {
    pub fn into_schema(self) -> ParsedSchemaQuery {
        match self {
            ParsedQuery::Schema(schema) => schema,
            ParsedQuery::Pipeline(_) => panic!("Expected a schema query, but got a data pipeline"),
        }
    }

    pub fn into_pipeline(self) -> ParsedPipeline {
        match self {
            ParsedQuery::Pipeline(pipeline) => pipeline,
            ParsedQuery::Schema(_) => panic!("Expected a data pipeline, but got a schema query"),
        }
    }
}

#[derive(Debug)]
pub struct ParsedSchemaQuery {
    query: SchemaQuery,
    source_query: Arc<String>,
    query_profile: QueryProfile,
}

impl ParsedSchemaQuery {
    pub fn new(query: SchemaQuery, source_query: Arc<String>, query_profile: QueryProfile) -> Self {
        Self { query, source_query, query_profile }
    }

    pub fn source_query(&self) -> &str {
        &self.source_query
    }
}

#[derive(Debug)]
pub struct ParsedPipeline {
    pipeline: Arc<typeql::query::Pipeline>,
    source_query: Arc<String>,
    query_profile: QueryProfile,
}

impl ParsedPipeline {
    pub fn new(pipeline: Arc<typeql::query::Pipeline>, source_query: Arc<String>, query_profile: QueryProfile) -> Self {
        Self { pipeline, source_query, query_profile }
    }

    pub fn source_query(&self) -> &str {
        &self.source_query
    }

    pub fn pipeline(&self) -> &typeql::query::Pipeline {
        &self.pipeline
    }
}

impl QueryManager {
    pub fn new(cache: Option<Arc<QueryCache>>) -> Self {
        Self { cache }
    }

    pub fn parse(&self, query: String) -> Result<ParsedQuery, Box<QueryError>> {
        self.parse_with_profile(query, QueryProfile::new(tracing::enabled!(Level::TRACE)))
    }

    pub fn parse_with_profile(&self, query: String, profile: QueryProfile) -> Result<ParsedQuery, Box<QueryError>> {
        profile.compilation_profile().set_stage_timer();
        if let Some(pipeline) = self.cache.as_ref().and_then(|cache| cache.get_parsed(&query)) {
            QUERY_PARSE_CACHE_HITS.increment();
            profile.compilation_profile().parsing_finished();
            return Ok(ParsedQuery::Pipeline(ParsedPipeline::new(pipeline, Arc::new(query), profile)));
        }
        let parsed = typeql::parse_query(&query)
            .map_err(|err| QueryError::ParseError { source_query: query.clone(), typedb_source: err })?;
        match parsed.into_structure() {
            QueryStructure::Schema(schema_query) => {
                profile.compilation_profile().parsing_finished();
                Ok(ParsedQuery::Schema(ParsedSchemaQuery::new(schema_query, Arc::new(query), profile)))
            }
            QueryStructure::Pipeline(pipeline) => {
                QUERY_PARSE_CACHE_MISSES.increment();
                let pipeline = Arc::new(pipeline);
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert_parsed(&query, pipeline.clone());
                }
                profile.compilation_profile().parsing_finished();
                Ok(ParsedQuery::Pipeline(ParsedPipeline::new(pipeline, Arc::new(query), profile)))
            }
        }
    }

    pub fn translate(
        &self,
        parsed: ParsedPipeline,
        snapshot: &impl ReadableSnapshot,
        function_manager: &FunctionManager,
        thing_manager: &ThingManager,
    ) -> Result<TranslatedPipeline, Box<QueryError>> {
        let ParsedPipeline { pipeline, source_query, query_profile } = parsed;
        query_profile.compilation_profile().set_stage_timer();
        let translated = match self.cache.as_ref().and_then(|cache| cache.get_translated(&source_query)) {
            Some(translated) => {
                QUERY_TRANSLATION_CACHE_HITS.increment();
                translated
            }
            None => {
                QUERY_TRANSLATION_CACHE_MISSES.increment();
                let translated =
                    translate_pipeline(snapshot, function_manager, &pipeline, source_query.clone(), query_profile)?;
                if let Some(cache) = self.cache.as_ref() {
                    cache.may_insert_translated(
                        thing_manager.statistics().sequence_number,
                        &source_query,
                        translated.clone(),
                    );
                }
                translated
            }
        };
        translated.query_context.profile.compilation_profile().translation_finished();
        Ok(translated)
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        function_manager: &FunctionManager,
        parsed: ParsedSchemaQuery,
    ) -> Result<(), Box<QueryError>> {
        let ParsedSchemaQuery {  query , source_query, query_profile, } = parsed;
        event!(Level::TRACE, "Running schema query:\n{}", query);
        let result = match &query {
            SchemaQuery::Define(define) => {
                let stage_profile = query_profile.profile_stage(|| String::from("Define"), 0); // TODO executable id
                let pattern_profile = stage_profile.create_or_get_pattern(|| String::from("Define pattern"));
                let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Define execution"));
                define::execute(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    define,
                    step_profile.storage_counters(),
                )
                .map_err(|err| Box::new(QueryError::Define { source_query: source_query.to_string(), typedb_source: err }))
            }
            SchemaQuery::Redefine(redefine) => {
                let stage_profile = query_profile.profile_stage(|| String::from("Redefine"), 0); // TODO executable id
                let pattern_profile = stage_profile.create_or_get_pattern(|| String::from("Redefine pattern"));
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
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, Box<QueryError>> {
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            query_context,
        } = pipeline;
        event!(Level::TRACE, "Preparing read query:\n{}", query_context.source_query);
        query_context.profile.compilation_profile().set_stage_timer();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = Arc::new(translated_given);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);
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
                    type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_context,
                    &mut variable_registry,
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

        let CompiledPipeline {
            executable_functions,
            executable_given,
            executable_stages,
            executable_fetch,
            pipeline_structure,
            ..
        } = executable_pipeline;
        let given_batch = validate_and_decode_given(executable_given.clone(), given_rows, &variable_registry)?;

        let execution_context = ExecutionContext::new(snapshot, thing_manager, function_manager);
        let query_context = Arc::new(query_context);
        // 4: Executor
        let pipeline = Pipeline::build_read_pipeline(
            execution_context,
            query_context.clone(),
            variable_registry.variable_names(),
            (variable_registry.branch_ids_allocated() < 64).then_some(pipeline_structure),
            Arc::new(executable_functions),
            executable_given,
            &executable_stages,
            executable_fetch,
            given_batch,
        )
        .map_err(|typedb_source| {
            Box::new(QueryError::Pipeline { source_query: query_context.source_query.as_ref().to_owned(), typedb_source })
        })?;
        Ok(pipeline)
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        pipeline: TranslatedPipeline,
        given_rows: Option<impl GivenRows>,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, Box<QueryError>)> {
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            query_context,
        } = pipeline;
        event!(Level::TRACE, "Preparing write query:\n{}", query_context.source_query);
        query_context.profile.compilation_profile().set_stage_timer();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = Arc::new(translated_given);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

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
                    type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_context,
                    &mut variable_registry,
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

        let CompiledPipeline {
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
        let execution_context = ExecutionContext::new(Arc::new(snapshot), thing_manager, function_manager);
        let query_context = Arc::new(query_context);
        let pipeline = Pipeline::build_write_pipeline(
            execution_context,
            query_context,
            variable_registry.variable_names(),
            (variable_registry.branch_ids_allocated() < 64).then_some(pipeline_structure),
            Arc::new(executable_functions),
            executable_given,
            executable_stages,
            executable_fetch,
            given_batch,
        );
        Ok(pipeline)
    }

    pub fn analyse<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        pipeline: TranslatedPipeline,
    ) -> Result<AnalysedQuery, Box<QueryError>> {
        let TranslatedPipeline {
            translated_preamble,
            translated_given,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            query_context,
        } = pipeline;
        event!(Level::TRACE, "Running analyse query:\n{}", query_context.source_query);
        query_context.profile.compilation_profile().set_stage_timer();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_given = translated_given.map(Arc::new);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

        match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
            Ok(_) => {}
            Err(typedb_source) => {
                return Err(Box::new(QueryError::FunctionDefinition {
                    source_query: query_context.source_query.as_ref().to_owned(),
                    typedb_source,
                }));
            }
        }
        query_context.profile.compilation_profile().validation_finished();

        // 2: Annotate
        let annotated_schema_functions =
            function_manager.get_annotated_functions(snapshot.as_ref(), type_manager).map_err(|err| {
                QueryError::FunctionDefinition { source_query: query_context.source_query.as_ref().to_owned(), typedb_source: *err }
            })?;

        let annotated_pipeline = annotate_preamble_and_pipeline(
            snapshot.as_ref(),
            type_manager,
            annotated_schema_functions.clone(),
            &mut variable_registry,
            query_context.parameters.as_ref(),
            (*arced_preamble).clone(),
            arced_given.map(|given| (*given).clone()),
            (*arced_stages).clone(),
            (*arced_fetch).clone(),
        )
        .map_err(|err| QueryError::Annotation { source_query: query_context.source_query.as_ref().to_owned(), typedb_source: err })?;
        query_context.profile.compilation_profile().annotation_finished();

        let query_structure = extract_query_structure_from(
            &variable_registry,
            query_context.parameters.clone(),
            &annotated_pipeline,
            &query_context.source_query,
        );
        let query_structure_annotations = QueryStructureAnnotations::build(
            snapshot.as_ref(),
            type_manager,
            &variable_registry,
            query_context.parameters.clone(),
            &query_context.source_query,
            &annotated_pipeline,
            &query_structure,
        )
        .map_err(|source| {
            Box::new(QueryError::QueryAnalysisFailed {
                source_query: query_context.source_query.as_ref().to_owned(),
                typedb_source: source,
            })
        })?;

        if query_context.profile.is_enabled() {
            event!(Level::INFO, "Analyse query done.\n{}", query_context.profile);
        }
        Ok(AnalysedQuery {
            source: query_context.source_query.clone(),
            structure: query_structure,
            annotations: query_structure_annotations,
        })
    }
}

fn translate_pipeline<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    function_manager: &FunctionManager,
    query: &typeql::query::Pipeline,
    source_query: Arc<String>,
    query_profile: QueryProfile,
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
    ir::translation::pipeline::translate_pipeline(&all_function_signatures, query, source_query.clone(), query_profile)
        .map_err(|err| {
            Box::new(QueryError::Representation { source_query: source_query.to_string(), typedb_source: err })
        })
}

fn annotate_and_compile_query(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_context: &QueryContext,
    variable_registry: &mut VariableRegistry,
    arced_preamble: Arc<Vec<Function>>,
    arced_given: Arc<Option<TranslatedGiven>>,
    arced_stages: Arc<Vec<TranslatedStage>>,
    arced_fetch: Arc<Option<FetchObject>>,
) -> Result<CompiledPipeline, Box<QueryError>> {
    if let Err(typedb_source) = validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
        return Err(Box::new(QueryError::FunctionDefinition { source_query: query_context.source_query.as_ref().to_owned(), typedb_source }));
    }
    query_context.profile.compilation_profile().validation_finished();

    // 2: Annotate
    let annotated_schema_functions = match function_manager.get_annotated_functions(snapshot, type_manager) {
        Ok(functions) => functions,
        Err(err) => {
            return Err(Box::new(QueryError::FunctionDefinition {
                source_query: query_context.source_query.as_ref().to_owned(),
                typedb_source: *err,
            }));
        }
    };

    let annotated_pipeline = annotate_preamble_and_pipeline(
        snapshot,
        type_manager,
        annotated_schema_functions.clone(),
        variable_registry,
        &query_context.parameters,
        (*arced_preamble).clone(),
        (*arced_given).clone(),
        (*arced_stages).clone(),
        (*arced_fetch).clone(),
    );

    let mut annotated_pipeline = match annotated_pipeline {
        Ok(annotated_pipeline) => annotated_pipeline,
        Err(err) => {
            return Err(Box::new(QueryError::Annotation {
                source_query: query_context.source_query.as_ref().to_owned(),
                typedb_source: err,
            }));
        }
    };
    query_context.profile.compilation_profile().annotation_finished();
    // TODO: We can avoid this for the regular query path when we break studio backwards compatibility
    let pipeline_structure = Arc::new(extract_pipeline_structure_from(
        variable_registry,
        annotated_pipeline.annotated_given.as_ref(),
        &annotated_pipeline.annotated_stages,
        &query_context.source_query,
    ));

    match apply_transformations(snapshot, type_manager, variable_registry, &mut annotated_pipeline) {
        Ok(_) => {}
        Err(err) => {
            return Err(Box::new(QueryError::Transformation {
                source_query: query_context.source_query.as_ref().to_owned(),
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
                source_query: query_context.source_query.as_ref().to_owned(),
                typedb_source: err,
            }));
        }
    };
    query_context.profile.compilation_profile().compilation_finished();
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
