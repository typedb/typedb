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
        type_annotations::{BlockAnnotations, TypeAnnotations},
    },
    query_structure::{
        ConjunctionAnnotations, PipelineStructure, PipelineStructureAnnotations, PipelineVariableAnnotation,
        PipelineVariableAnnotationAndModifier, QueryStructure, StageIndex, StructureVariableId,
    },
};
use concept::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, type_manager::TypeManager, OwnerAPI, TypeAPI},
};
use encoding::value::value_type::ValueType;
use ir::{
    pattern::{
        conjunction::Conjunction, nested_pattern::NestedPattern, ParameterID, Pattern, Scope, VariableBindingMode,
        Vertex,
    },
    pipeline::{ParameterRegistry, VariableRegistry},
};
use itertools::chain;
use storage::snapshot::ReadableSnapshot;

#[derive(Debug)]
pub struct AnalysedQuery {
    pub source: String,
    pub structure: QueryStructure,
    pub annotations: QueryStructureAnnotations,
}

#[derive(Debug)]
pub struct QueryStructureAnnotations {
    pub preamble: Vec<FunctionStructureAnnotations>,
    pub query: PipelineStructureAnnotations,
    pub fetch: Option<FetchStructureAnnotationsFields>,
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

pub type FetchStructureAnnotationsFields = HashMap<String, FetchStructureAnnotations>;

#[derive(Debug)]
pub enum FetchStructureAnnotations {
    Leaf(BTreeSet<ValueType>),
    Object(FetchStructureAnnotationsFields),
    List(Box<FetchStructureAnnotations>),
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
    let mut pipeline_annotations = Vec::with_capacity(structure.parametrised_structure.conjunctions.len());
    pipeline_annotations.resize(structure.parametrised_structure.conjunctions.len(), BTreeMap::new());
    stages.iter().enumerate().for_each(|(index, stage)| match stage {
        | AnnotatedStage::Match { block, block_annotations, .. }
        | AnnotatedStage::Put { block, match_annotations: block_annotations, .. }
        | AnnotatedStage::Insert { block, annotations: block_annotations, .. }
        | AnnotatedStage::Update { block, annotations: block_annotations, .. }
        | AnnotatedStage::Delete { block, annotations: block_annotations, .. } => {
            block_annotations.type_annotations().iter().for_each(|(_scope_id, _annotations)| {
                insert_pipeline_annotations_recursive(
                    variable_registry,
                    structure,
                    StageIndex(index),
                    block_annotations,
                    block.conjunction(),
                    &mut pipeline_annotations,
                )
            })
        }
        AnnotatedStage::Select(_)
        | AnnotatedStage::Sort(_)
        | AnnotatedStage::Offset(_)
        | AnnotatedStage::Limit(_)
        | AnnotatedStage::Require(_)
        | AnnotatedStage::Distinct(_)
        | AnnotatedStage::Reduce(_, _) => {}
    });
    pipeline_annotations
}

fn variable_annotations_for_block<'a>(
    variable_registry: &'a VariableRegistry,
    annotations_for_block: &'a TypeAnnotations,
) -> impl Iterator<Item = (Variable, PipelineVariableAnnotation)> + 'a {
    let concept_annotations = annotations_for_block.vertex_annotations().iter().filter_map(|(vertex, annos)| {
        let variable = vertex.as_variable()?;
        let category = variable_registry.get_variable_category(variable).unwrap();
        let annotations = match category.is_category_type() {
            true => PipelineVariableAnnotation::Type(annos.iter().copied().collect()),
            false => PipelineVariableAnnotation::Instance(annos.iter().copied().collect()),
        };
        Some((variable, annotations))
    });
    let value_annotations = annotations_for_block.value_annotations().iter().filter_map(|(vertex, annos)| {
        vertex.as_variable().map(|variable| (variable, PipelineVariableAnnotation::Value(annos.value_type().clone())))
    });
    concept_annotations.chain(value_annotations)
}

fn insert_pipeline_annotations_recursive(
    variable_registry: &VariableRegistry,
    structure: &PipelineStructure,
    stage_index: StageIndex,
    block_annotations: &BlockAnnotations,
    conjunction: &Conjunction,
    pipeline_annotations: &mut PipelineStructureAnnotations,
) {
    let block_id = structure.parametrised_structure.resolve_conjunction_id(stage_index, conjunction.scope_id());
    let variable_annotations =
        variable_annotations_for_block(variable_registry, block_annotations.type_annotations_of(conjunction).unwrap());
    pipeline_annotations[block_id.0 as usize] = enrich_annotations(conjunction, variable_annotations);

    conjunction.nested_patterns().iter().for_each(|nested| match nested {
        NestedPattern::Disjunction(branches) => branches.conjunctions().iter().for_each(|inner| {
            insert_pipeline_annotations_recursive(
                variable_registry,
                structure,
                stage_index,
                block_annotations,
                inner,
                pipeline_annotations,
            );
        }),
        NestedPattern::Negation(inner) => {
            insert_pipeline_annotations_recursive(
                variable_registry,
                structure,
                stage_index,
                block_annotations,
                inner.conjunction(),
                pipeline_annotations,
            );
        }
        NestedPattern::Optional(inner) => {
            insert_pipeline_annotations_recursive(
                variable_registry,
                structure,
                stage_index,
                block_annotations,
                inner.conjunction(),
                pipeline_annotations,
            );
        }
    });
}

fn enrich_annotations(
    conjunction: &Conjunction,
    variable_annotations: impl Iterator<Item = (Variable, PipelineVariableAnnotation)>,
) -> ConjunctionAnnotations {
    variable_annotations
        .map(|(variable, annotations)| {
            // TODO: We don't always have the info here :/
            let is_optional = conjunction
                .variable_binding_modes()
                .get(&variable)
                .map(VariableBindingMode::is_optionally_binding)
                .unwrap_or(false);
            (StructureVariableId::from(variable), PipelineVariableAnnotationAndModifier { is_optional, annotations })
        })
        .collect()
}

pub fn build_fetch_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    parameters: Arc<ParameterRegistry>,
    source_query: &str,
    last_stage_annotations: &TypeAnnotations,
    object: &AnnotatedFetchObject,
) -> Result<FetchStructureAnnotationsFields, Box<ConceptReadError>> {
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
) -> Result<FetchStructureAnnotationsFields, Box<ConceptReadError>> {
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
                    fetch_value_types.insert(label, FetchStructureAnnotations::Leaf(value_types));
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
) -> Result<FetchStructureAnnotationsFields, Box<ConceptReadError>> {
    entries.iter().map(|(parameter_id, fetch_object)| {
        let key = parameters.fetch_key(parameter_id).expect("Expected fetch key to be present").to_owned();
        let fetch_object_annotations_maybe_list = match fetch_object {
            AnnotatedFetchSome::SingleVar(var) => {
                let as_vertex = Vertex::Variable(*var);
                if let Some(annotations) = last_stage_annotations.vertex_annotations_of(&as_vertex) {
                    let attribute_types = annotations.iter().filter(|&attribute_type| attribute_type.is_attribute_type()).map(|attribute_type| attribute_type.as_attribute_type());
                    let leaf_annotations = build_leaf_annotations(snapshot, type_manager, attribute_types)?;
                    FetchStructureAnnotations::Leaf(leaf_annotations)
                } else if let Some(value_type) = last_stage_annotations.value_type_annotations_of(&as_vertex) {
                    FetchStructureAnnotations::Leaf(BTreeSet::from([value_type.value_type().clone()]))
                } else {
                    unreachable!("Expected either type annotations or value annotations to be present");
                }
            }
            AnnotatedFetchSome::ListAttributesAsList(_var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::ListAttributesFromList(_var, attribute_type) // TODO: Verify these can use the same code as SingleAttribute
            | AnnotatedFetchSome::SingleAttribute(_var, attribute_type) => {
                // TODO: Refine based on owner?
                let subtypes = attribute_type.get_subtypes(snapshot, type_manager)?;
                let attribute_types = chain!([*attribute_type].into_iter(), subtypes.iter().copied());
                FetchStructureAnnotations::Leaf(build_leaf_annotations(snapshot, type_manager, attribute_types)?)
            }
            AnnotatedFetchSome::Object(inner) => {
                FetchStructureAnnotations::Object(build_fetch_annotations(snapshot, type_manager, parameters.clone(), source_query, last_stage_annotations, inner)?)
            }
            AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
                let last_stage_annotations = get_last_stage_annotations(sub_fetch.stages.as_slice());
                let fetch = build_fetch_annotations(snapshot, type_manager, parameters.clone(), source_query, last_stage_annotations, &sub_fetch.fetch.object)?;
                FetchStructureAnnotations::Object(fetch)
            }
            AnnotatedFetchSome::ListFunction(function)
            | AnnotatedFetchSome::SingleFunction(function) => {
                debug_assert!(function.annotated_signature.returns.len() == 1);
                match &function.annotated_signature.returns[0] {
                    FunctionParameterAnnotation::AnyConcept => {
                        FetchStructureAnnotations::Leaf(
                            build_leaf_annotations(
                                snapshot,
                                type_manager,
                                type_manager.get_attribute_types(snapshot)?.into_iter()
                            )?
                        )
                    }
                    FunctionParameterAnnotation::Concept(types) => {
                        debug_assert!(types.iter().all(|type_| type_.is_attribute_type()));
                        FetchStructureAnnotations::Leaf(
                            build_leaf_annotations(
                                snapshot,
                                type_manager,
                                types.iter().copied().filter(|type_| type_.is_attribute_type()).map(|type_| type_.as_attribute_type())
                            )?
                        )
                    }
                    FunctionParameterAnnotation::Value(value_type) => {
                        FetchStructureAnnotations::Leaf(BTreeSet::from([value_type.clone()]))
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
                FetchStructureAnnotations::List(Box::new(fetch_object_annotations_maybe_list))
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
            | AnnotatedStage::Match { block_annotations, block, .. }
            | AnnotatedStage::Put { match_annotations: block_annotations, block, .. }
            | AnnotatedStage::Insert { annotations: block_annotations, block, .. }
            | AnnotatedStage::Update { annotations: block_annotations, block, .. } => {
                Some(block_annotations.type_annotations_of(block.conjunction()).unwrap())
            }
            | AnnotatedStage::Delete { .. }
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
