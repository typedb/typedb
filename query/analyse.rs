/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::variable::Variable;
use compiler::{
    annotation::{
        fetch::{AnnotatedFetchObject, AnnotatedFetchSome},
        function::{AnnotatedFunctionSignature, FunctionParameterAnnotation},
        pipeline::{AnnotatedPipeline, AnnotatedStage},
        type_annotations::TypeAnnotations,
    },
    query_structure::{
        PipelineStructure, PipelineStructureAnnotations, PipelineVariableAnnotation, QueryStructure,
        QueryStructureConjunctionID, StructureVariableId,
    },
};
use concept::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, type_manager::TypeManager, OwnerAPI, TypeAPI},
};
use encoding::value::value_type::ValueType;
use ir::{
    pattern::{ParameterID, Scope, Vertex},
    pipeline::{ParameterRegistry, VariableRegistry},
};
use itertools::chain;
use storage::snapshot::ReadableSnapshot;

#[derive(Debug)]
pub struct AnalysedQuery {
    pub structure: QueryStructure,
    pub annotations: QueryStructureAnnotations,
}

#[derive(Debug)]
pub struct QueryStructureAnnotations {
    pub preamble: Vec<FunctionStructureAnnotations>,
    pub query: PipelineStructureAnnotations,
    pub fetch: Option<FetchStructureAnnotations>,
}

impl QueryStructureAnnotations {
    pub fn build<Snapshot: ReadableSnapshot>(
        snapshot: &Snapshot,
        type_manager: &TypeManager,
        variable_registry: &VariableRegistry,
        parameters: Arc<ParameterRegistry>,
        source_query: &str,
        annotated_pipeline: &AnnotatedPipeline,
        query_structure: &QueryStructure,
    ) -> Result<Self, Box<ConceptReadError>> {
        let AnnotatedPipeline { annotated_stages, annotated_fetch, annotated_preamble } = &annotated_pipeline;
        let pipeline =
            build_pipeline_annotations(variable_registry, annotated_stages.as_slice(), &query_structure.query);
        let last_stage_annotations = get_last_stage_annotations(annotated_stages.as_slice());
        let fetch = annotated_fetch
            .as_ref()
            .map(|fetch| {
                build_fetch_annotations(
                    snapshot,
                    type_manager,
                    parameters.clone(),
                    source_query,
                    last_stage_annotations,
                    &fetch.object,
                )
            })
            .transpose()?;
        let preamble = annotated_preamble
            .iter()
            .zip(query_structure.preamble.iter())
            .map(|(annotated_function, structure)| {
                let signature = annotated_function.annotated_signature.clone();
                let body = build_pipeline_annotations(
                    &annotated_function.variable_registry,
                    annotated_function.stages.as_slice(),
                    &structure.body,
                );
                FunctionStructureAnnotations { signature, body }
            })
            .collect();

        Ok(Self { preamble, query: pipeline, fetch })
    }
}

pub type FetchStructureAnnotations = HashMap<String, FetchObjectStructureAnnotations>;

#[derive(Debug)]
pub enum FetchObjectStructureAnnotations {
    Leaf(BTreeSet<ValueType>),
    Object(FetchStructureAnnotations),
    List(Box<FetchObjectStructureAnnotations>),
}

#[derive(Debug)]
pub struct FunctionStructureAnnotations {
    pub signature: AnnotatedFunctionSignature,
    pub body: PipelineStructureAnnotations,
}

pub fn build_pipeline_annotations(
    variable_registry: &VariableRegistry,
    stages: &[AnnotatedStage],
    structure: &PipelineStructure,
) -> PipelineStructureAnnotations {
    fn insert_variable_annotations(
        variable_registry: &VariableRegistry,
        variable_annotations: &mut PipelineStructureAnnotations,
        block_id: QueryStructureConjunctionID,
        annotations_for_block: &TypeAnnotations,
    ) {
        let mut annotations : BTreeMap<_,_> = annotations_for_block
            .vertex_annotations()
            .iter()
            .filter_map(|(vertex, annos)| {
                vertex.as_variable().map(|variable| {
                    let category = variable_registry.get_variable_category(variable).unwrap();
                    let annotations = match category.is_category_type() {
                        true => PipelineVariableAnnotation::Type(annos.iter().copied().collect()),
                        false => PipelineVariableAnnotation::Thing(annos.iter().copied().collect()),
                    };
                    (StructureVariableId::from(variable), annotations)
                })
            })
            .collect();
        annotations.extend(annotations_for_block.value_annotations().iter().filter_map(|(vertex, annos)| {
            vertex.as_variable().map(|variable| (
                StructureVariableId::from(variable),
                PipelineVariableAnnotation::Value(annos.value_type().clone())
            ))
        }));
        variable_annotations.insert(block_id, annotations);
    }
    let mut variable_annotations = BTreeMap::new();
    stages.iter().for_each(|stage| match stage {
        AnnotatedStage::Put { match_annotations: block_annotations, .. }
        | AnnotatedStage::Match { block_annotations, .. } => {
            block_annotations.type_annotations().iter().for_each(|(scope_id, annotations)| {
                let block_id = structure.parametrised_structure.scope_to_conjunction_id.get(scope_id).unwrap().clone();
                insert_variable_annotations(variable_registry, &mut variable_annotations, block_id, annotations);
            })
        }
        AnnotatedStage::Insert { block, annotations, .. } | AnnotatedStage::Update { block, annotations, .. } => {
            let block_id = structure
                .parametrised_structure
                .scope_to_conjunction_id
                .get(&block.conjunction().scope_id())
                .unwrap()
                .clone();
            insert_variable_annotations(variable_registry, &mut variable_annotations, block_id, annotations);
        }
        AnnotatedStage::Delete { .. }
        | AnnotatedStage::Select(_)
        | AnnotatedStage::Sort(_)
        | AnnotatedStage::Offset(_)
        | AnnotatedStage::Limit(_)
        | AnnotatedStage::Require(_)
        | AnnotatedStage::Distinct(_)
        | AnnotatedStage::Reduce(_, _) => {}
    });
    variable_annotations
}

pub fn build_fetch_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    parameters: Arc<ParameterRegistry>,
    source_query: &str,
    last_stage_annotations: &TypeAnnotations,
    object: &AnnotatedFetchObject,
) -> Result<FetchStructureAnnotations, Box<ConceptReadError>> {
    match object {
        AnnotatedFetchObject::Entries(entries) => build_fetch_entries_annotations(
            snapshot,
            type_manager,
            parameters,
            source_query,
            last_stage_annotations,
            entries,
        ),
        AnnotatedFetchObject::Attributes(variable) => {
            build_fetch_attributes_annotations(snapshot, type_manager, last_stage_annotations, *variable)
        }
    }
}

fn build_fetch_attributes_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    last_stage_annotations: &TypeAnnotations,
    variable: Variable,
) -> Result<FetchStructureAnnotations, Box<ConceptReadError>> {
    let mut fetch_value_types = HashMap::new();
    let owner_types = last_stage_annotations
        .vertex_annotations_of(&Vertex::Variable(variable))
        .expect("Expected annotations to be available");
    owner_types.iter().filter(|owner_type| owner_type.is_entity_type() || owner_type.is_relation_type()).try_for_each(
        |owner_type| {
            let attribute_types = owner_type.as_object_type().get_owned_attribute_types(snapshot, type_manager)?;
            attribute_types.iter().try_for_each(|attribute_type| {
                let value_types = build_leaf_annotations(snapshot, type_manager, [*attribute_type].into_iter())?;
                if !value_types.is_empty() {
                    let label: String =
                        attribute_type.get_label(snapshot, type_manager)?.scoped_name.as_str().to_owned();
                    fetch_value_types.insert(label, FetchObjectStructureAnnotations::Leaf(value_types));
                }
                Ok::<(), Box<ConceptReadError>>(())
            })
        },
    )?;
    Ok(fetch_value_types)
}

fn build_fetch_entries_annotations<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    parameters: Arc<ParameterRegistry>,
    source_query: &str,
    last_stage_annotations: &TypeAnnotations,
    entries: &HashMap<ParameterID, AnnotatedFetchSome>,
) -> Result<FetchStructureAnnotations, Box<ConceptReadError>> {
    entries.iter().map(|(parameter_id, fetch_object)| {
        let key = parameters.fetch_key(*parameter_id).expect("Expected fetch key to be present").to_owned();
        let fetch_object_annotations_maybe_list = match fetch_object {
            AnnotatedFetchSome::SingleVar(var) => {
                let as_vertex = Vertex::Variable(*var);
                if let Some(annotations) = last_stage_annotations.vertex_annotations_of(&as_vertex) {
                    let attribute_types = annotations.iter().filter_map(|attribute_type| {
                        attribute_type.is_attribute_type().then(|| attribute_type.as_attribute_type())
                    });
                    let leaf_annotations = build_leaf_annotations(snapshot, type_manager, attribute_types)?;
                    FetchObjectStructureAnnotations::Leaf(leaf_annotations)
                } else if let Some(value_type) = last_stage_annotations.value_type_annotations_of(&as_vertex) {
                    FetchObjectStructureAnnotations::Leaf(BTreeSet::from([value_type.value_type().clone()]))
                } else {
                    unreachable!("Expected either type annotations or value annotations to be present");
                }
            }
            AnnotatedFetchSome::ListAttributesAsList(var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::ListAttributesFromList(var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::SingleAttribute(var, attribute_type) => {
                // TODO: Refine based on owner?
                let subtypes = attribute_type.get_subtypes(snapshot, type_manager)?;
                let attribute_types = chain!([*attribute_type].into_iter(), subtypes.iter().copied());
                FetchObjectStructureAnnotations::Leaf(build_leaf_annotations(snapshot, type_manager, attribute_types)?)
            }
            AnnotatedFetchSome::Object(inner) => {
                FetchObjectStructureAnnotations::Object(build_fetch_annotations(snapshot, type_manager, parameters.clone(), source_query, last_stage_annotations, inner)?)
            }
            AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
                let last_stage_annotations = get_last_stage_annotations(sub_fetch.stages.as_slice());
                let fetch = build_fetch_annotations(snapshot, type_manager, parameters.clone(), source_query, last_stage_annotations, &sub_fetch.fetch.object)?;
                FetchObjectStructureAnnotations::Object(fetch)
            }
            AnnotatedFetchSome::ListFunction(function)
            | AnnotatedFetchSome::SingleFunction(function) => {
                debug_assert!(function.annotated_signature.returns.len() == 1);
                match &function.annotated_signature.returns[0] {
                    FunctionParameterAnnotation::Concept(types) => {
                        debug_assert!(types.iter().all(|type_| type_.is_attribute_type()));
                        FetchObjectStructureAnnotations::Leaf(
                            build_leaf_annotations(
                                snapshot,
                                type_manager,
                                types.iter().copied().filter_map(|type_| type_.is_attribute_type().then(|| type_.as_attribute_type()))
                            )?
                        )
                    }
                    FunctionParameterAnnotation::Value(value_type) => {
                        FetchObjectStructureAnnotations::Leaf(BTreeSet::from([value_type.clone()]))
                    }
                }
            }
        };
        let fetch_object_annotations = match fetch_object {
            AnnotatedFetchSome::SingleVar(_)
            | AnnotatedFetchSome::SingleAttribute(_, _)
            | AnnotatedFetchSome::SingleFunction(_)
            | AnnotatedFetchSome::Object(_) => fetch_object_annotations_maybe_list,
            AnnotatedFetchSome::ListFunction(_)
            | AnnotatedFetchSome::ListSubFetch(_)
            | AnnotatedFetchSome::ListAttributesAsList(_, _)
            | AnnotatedFetchSome::ListAttributesFromList(_, _) => {
                FetchObjectStructureAnnotations::List(Box::new(fetch_object_annotations_maybe_list))
            }
        };
        Ok((key, fetch_object_annotations))
    }).collect::<Result<HashMap<_, _>, _>>()
}

fn build_leaf_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    attribute_types: impl Iterator<Item = AttributeType>,
) -> Result<BTreeSet<ValueType>, Box<ConceptReadError>> {
    attribute_types
        .filter_map(|attribute_type| {
            attribute_type
                .get_value_type(snapshot, type_manager)
                .map(|ok| ok.map(|(value_type, _)| value_type))
                .transpose()
        })
        .collect::<Result<BTreeSet<_>, _>>()
}

pub fn get_last_stage_annotations(stages: &[AnnotatedStage]) -> &TypeAnnotations {
    stages
        .iter()
        .filter_map(|stage| match stage {
            AnnotatedStage::Match { block_annotations, block, .. }
            | AnnotatedStage::Put { match_annotations: block_annotations, block, .. } => {
                Some(block_annotations.type_annotations_of(block.conjunction()).unwrap())
            }
            AnnotatedStage::Insert { annotations, .. } | AnnotatedStage::Update { annotations, .. } => {
                Some(annotations)
            }
            AnnotatedStage::Delete { .. }
            | AnnotatedStage::Select(_)
            | AnnotatedStage::Sort(_)
            | AnnotatedStage::Offset(_)
            | AnnotatedStage::Limit(_)
            | AnnotatedStage::Require(_)
            | AnnotatedStage::Distinct(_)
            | AnnotatedStage::Reduce(_, _) => None,
        })
        .last()
        .expect("Expected pipeline to have a last stage")
}
