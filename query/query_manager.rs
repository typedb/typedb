/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};
use std::collections::{BTreeSet, HashMap};

use compiler::{
    annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline},
    executable::pipeline::{compile_pipeline_and_functions, ExecutablePipeline},
    query_structure::extract_query_structure_from,
    transformation::transform::apply_transformations,
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    pipeline::Pipeline,
    stage::{ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex, validate_no_cycles};
use ir::{
    pipeline::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use resource::{
    perf_counters::{QUERY_CACHE_HITS, QUERY_CACHE_MISSES},
    profile::QueryProfile,
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use tracing::{event, Level};
use typeql::query::SchemaQuery;
use compiler::annotation::fetch::{AnnotatedFetch, AnnotatedFetchObject, AnnotatedFetchSome};
use compiler::annotation::pipeline::AnnotatedStage;
use compiler::query_structure::{ParametrisedQueryStructure, QueryStructure};
use concept::error::ConceptReadError;
use concept::type_::{OwnerAPI, TypeAPI};
use encoding::value::value_type::ValueType;
use executor::document::ConceptDocument;
use ir::pattern::{ParameterID, Vertex};
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use serde::{Deserialize, Serialize};
use answer::variable::Variable;
use compiler::annotation::function::FunctionParameterAnnotation;
use compiler::annotation::type_annotations::TypeAnnotations;
use concept::type_::attribute_type::AttributeType;

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
        } = self.translate_pipeline(snapshot.as_ref(), function_manager, query, source_query)?;
        compile_profile.translation_finished();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

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
                .map_err(|err| QueryError::Annotation { source_query: source_query.to_string(), typedb_source: err })?;
                compile_profile.annotation_finished();
                let query_structure = extract_query_structure_from(
                    &variable_registry,
                    &annotated_pipeline.annotated_stages,
                    source_query,
                )
                .map(Arc::new);

                apply_transformations(snapshot.as_ref(), type_manager, &mut annotated_pipeline).map_err(|err| {
                    QueryError::Transformation { source_query: source_query.to_string(), typedb_source: err }
                })?;

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
                    query_structure,
                )
                .map_err(|err| QueryError::ExecutableCompilation {
                    source_query: source_query.to_string(),
                    typedb_source: err,
                })?;
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert(arced_preamble, arced_stages, arced_fetch, executable_pipeline.clone())
                }
                compile_profile.compilation_finished();
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch, query_structure, .. } =
            executable_pipeline;

        // 4: Executor
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.variable_names(),
            query_structure,
            Arc::new(executable_functions),
            &executable_stages,
            executable_fetch,
            Arc::new(parameters),
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
        } = match self.translate_pipeline(&snapshot, function_manager, query, source_query) {
            Ok(translated) => translated,
            Err(err) => return Err((snapshot, err)),
        };
        compile_profile.translation_finished();
        let arced_preamble = Arc::new(translated_preamble);
        let arced_stages = Arc::new(translated_stages);
        let arced_fetch = Arc::new(translated_fetch);

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
                match validate_no_cycles(&arced_preamble.iter().enumerate().collect()) {
                    Ok(_) => {}
                    Err(typedb_source) => {
                        return Err((
                            snapshot,
                            Box::new(QueryError::FunctionDefinition {
                                source_query: source_query.to_string(),
                                typedb_source,
                            }),
                        ))
                    }
                }
                compile_profile.validation_finished();

                // 2: Annotate
                let annotated_schema_functions = match function_manager.get_annotated_functions(&snapshot, type_manager)
                {
                    Ok(functions) => functions,
                    Err(err) => {
                        return Err((
                            snapshot,
                            Box::new(QueryError::FunctionDefinition {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            }),
                        ))
                    }
                };

                let annotated_pipeline = annotate_preamble_and_pipeline(
                    &snapshot,
                    type_manager,
                    annotated_schema_functions.clone(),
                    &mut variable_registry,
                    &value_parameters,
                    (*arced_preamble).clone(),
                    (*arced_stages).clone(),
                    (*arced_fetch).clone(),
                );

                let mut annotated_pipeline = match annotated_pipeline {
                    Ok(annotated_pipeline) => annotated_pipeline,
                    Err(err) => {
                        return Err((
                            snapshot,
                            Box::new(QueryError::Annotation {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            }),
                        ))
                    }
                };
                compile_profile.annotation_finished();

                let query_structure = extract_query_structure_from(
                    &variable_registry,
                    &annotated_pipeline.annotated_stages,
                    source_query,
                )
                .map(Arc::new);

                match apply_transformations(&snapshot, type_manager, &mut annotated_pipeline) {
                    Ok(_) => {}
                    Err(err) => {
                        return Err((
                            snapshot,
                            Box::new(QueryError::Transformation {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            }),
                        ))
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
                    query_structure,
                ) {
                    Ok(executable) => executable,
                    Err(err) => {
                        return Err((
                            snapshot,
                            Box::new(QueryError::ExecutableCompilation {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            }),
                        ))
                    }
                };
                if let Some(cache) = self.cache.as_ref() {
                    cache.insert(arced_preamble, arced_stages, arced_fetch, executable_pipeline.clone())
                }
                compile_profile.compilation_finished();
                QUERY_CACHE_MISSES.increment();
                executable_pipeline
            }
        };

        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch, query_structure, .. } =
            executable_pipeline;

        // 4: Executor
        Ok(Pipeline::build_write_pipeline(
            snapshot,
            variable_registry.variable_names(),
            query_structure,
            thing_manager,
            Arc::new(executable_functions),
            executable_stages,
            executable_fetch,
            Arc::new(value_parameters),
            Arc::new(query_profile),
        ))
    }

    fn translate_pipeline<Snapshot: ReadableSnapshot>(
        &self,
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
        translate_pipeline(snapshot, &all_function_signatures, query).map_err(|err| {
            Box::new(QueryError::Representation { source_query: source_query.to_string(), typedb_source: err })
        })
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
        } = self.translate_pipeline(snapshot.as_ref(), function_manager, query, source_query)?;
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

        AnalysedQuery::build(snapshot.as_ref(), type_manager, &variable_registry, &parameters, source_query, annotated_pipeline).map_err(|source| {
            Box::new(QueryError::QueryAnalysisFailed { source_query: source_query.to_owned(), typedb_source: source } )
        })
    }
}


pub type AnalysedValueType = String;
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum AnalysedFetchObject {
    Leaf(Vec<AnalysedValueType>),
    Inner(HashMap<String, AnalysedFetchObject>)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalysedQuery {
    pub fetch: Option<HashMap<String, AnalysedFetchObject>>,
}

impl AnalysedQuery {
    pub fn build<Snapshot: ReadableSnapshot>(
        snapshot: &Snapshot,
        type_manager: &TypeManager,
        _variable_registry: &VariableRegistry,
        parameters: &ParameterRegistry,
        source_query: &str,
        annotated_pipeline: AnnotatedPipeline
    ) -> Result<Self, Box<ConceptReadError>> {
        Self::build_impl(snapshot, type_manager, _variable_registry, parameters, &annotated_pipeline.annotated_stages, annotated_pipeline.annotated_fetch.as_ref())
    }

    fn build_impl(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        _variable_registry: &VariableRegistry,
        parameters: &ParameterRegistry,
        stages: &[AnnotatedStage],
        fetch: Option<&AnnotatedFetch>
    ) -> Result<Self, Box<ConceptReadError>> {
        // TODO: Consider adding query structure and so on
        let stage_annotations = stages.iter().enumerate().filter_map(|(i,stage)| {
            match stage {
                AnnotatedStage::Match { block_annotations, block, .. }
                | AnnotatedStage::Put { match_annotations: block_annotations, block, .. } => {
                    Some((i, block_annotations.type_annotations_of(block.conjunction()).unwrap()))
                }
                AnnotatedStage::Insert { annotations, .. } | AnnotatedStage::Update { annotations, .. } => {
                    Some((i, annotations))
                }
                AnnotatedStage::Delete { .. }
                | AnnotatedStage::Select(_)
                | AnnotatedStage::Sort(_)
                | AnnotatedStage::Offset(_)
                | AnnotatedStage::Limit(_)
                | AnnotatedStage::Require(_)
                | AnnotatedStage::Distinct(_)
                | AnnotatedStage::Reduce(_, _) => None,
            }
        }).collect::<Vec<_>>();
        let last_stage_annotations = &stage_annotations.last().expect("Expected pipeline to have a last stage").1;
        let fetch = fetch.map(|fetch| {
            encode_analysed_fetch_object(snapshot, type_manager, parameters, last_stage_annotations, &fetch.object)
        }).transpose()?;
        Ok(Self { fetch })
    }
}

fn encode_analysed_fetch_object(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    parameters: &ParameterRegistry,
    last_stage_annotations: &TypeAnnotations,
    object: &AnnotatedFetchObject,
) -> Result<HashMap<String, AnalysedFetchObject>, Box<ConceptReadError>> {
    match object {
        AnnotatedFetchObject::Entries(entries) => {
            encode_analysed_fetch_entries(snapshot, type_manager, parameters, last_stage_annotations, entries)
        },
        AnnotatedFetchObject::Attributes(variable) => {
            encode_analysed_fetch_attributes(snapshot, type_manager, last_stage_annotations, *variable)
        }
    }
}

fn encode_analysed_fetch_attributes(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    last_stage_annotations: &TypeAnnotations,
    variable: Variable,
) -> Result<HashMap<String, AnalysedFetchObject>, Box<ConceptReadError>> {
    let mut fetch_value_types = HashMap::new();
    let owner_types = last_stage_annotations.vertex_annotations_of(&Vertex::Variable(variable))
        .expect("Expected annotations to be available");
    owner_types.iter().filter(|owner_type| owner_type.is_entity_type() || owner_type.is_relation_type())
        .try_for_each(|owner_type| {
            let attribute_types = owner_type.as_object_type().get_owned_attribute_types(snapshot, type_manager)?;
            attribute_types.iter()
                .try_for_each(|attribute_type| {
                    let value_types = encode_leaf(snapshot, type_manager, [*attribute_type].into_iter())?;
                    if !value_types.is_empty() {
                        let label: String = attribute_type.get_label(snapshot, type_manager)?.scoped_name.as_str().to_owned();
                        fetch_value_types.insert(label, AnalysedFetchObject::Leaf(value_types));
                    }
                    Ok::<(), Box<ConceptReadError>>(())
                })
        })?;
    Ok(fetch_value_types)
}

fn encode_analysed_fetch_entries<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    parameters: &ParameterRegistry,
    last_stage_annotations: &TypeAnnotations,
    entries: &HashMap<ParameterID, AnnotatedFetchSome>
) -> Result<HashMap<String, AnalysedFetchObject>, Box<ConceptReadError>> {
    entries.iter().map(|(parameter_id, fetch_object)| {
        let key = parameters.fetch_key(*parameter_id).expect("Expected fetch key to be present").to_owned();
        let analysed_object = match fetch_object {
            AnnotatedFetchSome::SingleVar(var) => {
                let attribute_types = last_stage_annotations.vertex_annotations_of(&Vertex::Variable(*var))
                    .expect("Expected annotations to be present").iter()
                    .filter_map(|attribute_type| {
                        attribute_type.is_attribute_type().then(|| attribute_type.as_attribute_type())
                    });
                AnalysedFetchObject::Leaf(encode_leaf(snapshot, type_manager, attribute_types)?)
            }
            AnnotatedFetchSome::ListAttributesAsList(var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::ListAttributesFromList(var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::SingleAttribute(var, attribute_type) => {
                // TODO: Refine based on variable
                let attribute_types = attribute_type.get_subtypes(snapshot, type_manager)?;
                AnalysedFetchObject::Leaf(encode_leaf(snapshot, type_manager, attribute_types.iter().copied())?)
            }
            AnnotatedFetchSome::Object(inner) => {
                AnalysedFetchObject::Inner(encode_analysed_fetch_object(snapshot, type_manager, parameters, last_stage_annotations, inner)?)
            }
            AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
                let built = AnalysedQuery::build_impl(snapshot, type_manager, &sub_fetch.variable_registry, parameters, &sub_fetch.stages, Some(&sub_fetch.fetch))?;
                AnalysedFetchObject::Inner(built.fetch.unwrap())
            }
            AnnotatedFetchSome::ListFunction(function)
            | AnnotatedFetchSome::SingleFunction(function) => {
                debug_assert!(function.annotated_signature.returned.len() == 1);
                match &function.annotated_signature.returned[0] {
                    FunctionParameterAnnotation::Concept(types) => {
                        debug_assert!(types.iter().all(|type_| type_.is_attribute_type()));
                        AnalysedFetchObject::Leaf(
                            encode_leaf(
                                snapshot,
                                type_manager,
                                types.iter().copied().filter_map(|type_| type_.is_attribute_type().then(|| type_.as_attribute_type()))
                            )?
                        )
                    }
                    FunctionParameterAnnotation::Value(value_type) => {
                        AnalysedFetchObject::Leaf(vec![value_type.to_string()])
                    }
                }
            }
        };
        Ok((key, analysed_object))
    }).collect::<Result<HashMap<_, _>, _>>()
}

fn encode_leaf(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    attribute_types: impl Iterator<Item = AttributeType>,
) -> Result<Vec<AnalysedValueType>, Box<ConceptReadError>> {
    attribute_types
        .filter_map(|attribute_type| {
            attribute_type.get_value_type(snapshot, type_manager)
                .map(|ok| ok.map(|(value_type, _)| value_type.to_string()))
                .transpose()
        }).collect::<Result<Vec<_>,_>>()
}
