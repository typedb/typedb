/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::Type;
use compiler::{
    annotation::function::FunctionParameterAnnotation,
    query_structure::{
        FunctionStructure, PipelineVariableAnnotation, PipelineVariableAnnotationAndModifier, StructureVariableId,
    },
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use query::analyse::{FetchStructureAnnotations, FetchStructureAnnotationsFields, FunctionStructureAnnotations};
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::{
    concept::{
        encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_value_type,
        AttributeTypeResponse, EntityTypeResponse, RelationTypeResponse, RoleTypeResponse,
    },
    query_structure::{encode_analyzed_pipeline, AnalyzedFunctionResponse},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub(super) enum SingleTypeAnnotationResponse {
    Entity(EntityTypeResponse),
    Relation(RelationTypeResponse),
    Attribute(AttributeTypeResponse),
    Role(RoleTypeResponse),
}

impl SingleTypeAnnotationResponse {
    pub fn label(&self) -> &'_ str {
        match self {
            SingleTypeAnnotationResponse::Entity(entity) => &entity.label,
            SingleTypeAnnotationResponse::Relation(relation) => &relation.label,
            SingleTypeAnnotationResponse::Attribute(attribute) => &attribute.label,
            SingleTypeAnnotationResponse::Role(role) => &role.label,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VariableAnnotationsResponse {
    is_optional: bool,
    #[serde(flatten)]
    annotations: TypeAnnotationResponse,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(super) enum TypeAnnotationResponse {
    Thing {
        annotations: Vec<SingleTypeAnnotationResponse>,
    },
    Type {
        annotations: Vec<SingleTypeAnnotationResponse>,
    },
    #[serde(rename_all = "camelCase")]
    Value {
        value_types: Vec<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConjunctionAnnotationsResponse {
    pub(super) variable_annotations: HashMap<StructureVariableId, VariableAnnotationsResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(super) enum FunctionReturnAnnotationsResponse {
    Single { annotations: Vec<TypeAnnotationResponse> },
    Stream { annotations: Vec<TypeAnnotationResponse> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum FetchStructureAnnotationsResponse {
    #[serde(rename_all = "camelCase")]
    Value {
        value_types: Vec<String>,
    }, // Value types encoded as string
    #[serde(rename_all = "camelCase")]
    Object {
        possible_fields: Vec<FetchStructureFieldAnnotationsResponse>,
    },
    List {
        elements: Box<FetchStructureAnnotationsResponse>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchStructureFieldAnnotationsResponse {
    key: String,
    #[serde(flatten)]
    value: FetchStructureAnnotationsResponse,
}

fn encode_function_parameter_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotation: FunctionParameterAnnotation,
) -> Result<TypeAnnotationResponse, Box<ConceptReadError>> {
    Ok(match annotation {
        FunctionParameterAnnotation::Concept(types) => TypeAnnotationResponse::Thing {
            annotations: encode_type_annotation_vec(snapshot, type_manager, types.iter())?,
        },
        FunctionParameterAnnotation::Value(v) => {
            TypeAnnotationResponse::Value { value_types: vec![encode_value_type(v, snapshot, type_manager)?] }
        }
    })
}

pub(super) fn encode_variable_type_annotations_and_modifiers(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotations_modifiers: &PipelineVariableAnnotationAndModifier,
) -> Result<VariableAnnotationsResponse, Box<ConceptReadError>> {
    let annotations = match &annotations_modifiers.annotations {
        PipelineVariableAnnotation::Type(types) => TypeAnnotationResponse::Type {
            annotations: encode_type_annotation_vec(snapshot, type_manager, types.iter())?,
        },
        PipelineVariableAnnotation::Thing(types) => TypeAnnotationResponse::Thing {
            annotations: encode_type_annotation_vec(snapshot, type_manager, types.iter())?,
        },
        PipelineVariableAnnotation::Value(v) => {
            let value_types = vec![encode_value_type(v.clone(), snapshot, type_manager)?];
            TypeAnnotationResponse::Value { value_types }
        }
    };
    let is_optional = annotations_modifiers.is_optional;
    Ok(VariableAnnotationsResponse { is_optional, annotations })
}

fn encode_type_annotation_vec<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    types: impl Iterator<Item = &'a Type>,
) -> Result<Vec<SingleTypeAnnotationResponse>, Box<ConceptReadError>> {
    let mut encoded =
        types.map(|t| encode_type_annotation(snapshot, type_manager, t)).collect::<Result<Vec<_>, _>>()?;
    encoded.sort_by(|a, b| a.label().cmp(b.label()));
    Ok(encoded)
}

fn encode_type_annotation(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: &Type,
) -> Result<SingleTypeAnnotationResponse, Box<ConceptReadError>> {
    match type_ {
        Type::Entity(entity) => {
            Ok(SingleTypeAnnotationResponse::Entity(encode_entity_type(entity, snapshot, type_manager)?))
        }
        Type::Relation(relation) => {
            Ok(SingleTypeAnnotationResponse::Relation(encode_relation_type(relation, snapshot, type_manager)?))
        }
        Type::Attribute(attribute) => {
            Ok(SingleTypeAnnotationResponse::Attribute(encode_attribute_type(attribute, snapshot, type_manager)?))
        }
        Type::RoleType(role) => Ok(SingleTypeAnnotationResponse::Role(encode_role_type(role, snapshot, type_manager)?)),
    }
}

pub(super) fn encode_analyzed_function(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: FunctionStructure,
    annotations: FunctionStructureAnnotations,
) -> Result<AnalyzedFunctionResponse, Box<ConceptReadError>> {
    let arguments = structure.arguments;
    let returns = structure.return_;
    let body = encode_analyzed_pipeline(snapshot, type_manager, &structure.body, &annotations.body, true)?;
    let argument_annotations = annotations.signature.arguments.into_iter().map(|arg| {
        encode_function_parameter_annotations(snapshot, type_manager, arg)
    }).collect::<Result<Vec<_>, _>>()?;
    let return_types = annotations.signature.returns.into_iter().map(|arg| {
        encode_function_parameter_annotations(snapshot, type_manager, arg)
    }).collect::<Result<Vec<_>, _>>()?;
    let return_annotations = match annotations.signature.is_stream {
        true => FunctionReturnAnnotationsResponse::Stream { annotations: return_types },
        false => FunctionReturnAnnotationsResponse::Single { annotations: return_types },
    };
    Ok(AnalyzedFunctionResponse { arguments, returns, body, argument_annotations, return_annotations })
}

pub(super) fn encode_analyzed_fetch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    fetch_structure_annotations: FetchStructureAnnotationsFields,
) -> Result<Vec<FetchStructureFieldAnnotationsResponse>, Box<ConceptReadError>> {
    fetch_structure_annotations
        .into_iter()
        .map(|(key, object)| {
            encode_fetch_object_structure_annotations(snapshot, type_manager, object)
                .map(|value| FetchStructureFieldAnnotationsResponse { key, value })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn encode_fetch_object_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    fetch_object_structure_annotations: FetchStructureAnnotations,
) -> Result<FetchStructureAnnotationsResponse, Box<ConceptReadError>> {
    // TODO: We don't encode the pipeline anywhere
    let encoded = match fetch_object_structure_annotations {
        FetchStructureAnnotations::Leaf(leaf) => {
            let value_types = leaf
                .into_iter()
                .map(|v| encode_value_type(v, snapshot, type_manager))
                .collect::<Result<Vec<_>, _>>()?;
            FetchStructureAnnotationsResponse::Value { value_types }
        }
        FetchStructureAnnotations::Object(object) => FetchStructureAnnotationsResponse::Object {
            possible_fields: encode_analyzed_fetch(snapshot, type_manager, object)?,
        },
        FetchStructureAnnotations::List(boxed_list_annotations) => {
            let elements = encode_fetch_object_structure_annotations(snapshot, type_manager, *boxed_list_annotations)?;
            FetchStructureAnnotationsResponse::List { elements: Box::new(elements) }
        }
    };
    Ok(encoded)
}

#[rustfmt::skip]
#[cfg(debug_assertions)]
pub mod bdd {
    use std::collections::HashMap;

    use compiler::query_structure::{QueryStructureConjunctionID, QueryStructureStage};
    use itertools::Itertools;

    use super::{FetchStructureAnnotationsResponse, FunctionReturnAnnotationsResponse, SingleTypeAnnotationResponse, TypeAnnotationResponse, ConjunctionAnnotationsResponse, VariableAnnotationsResponse};
    use crate::service::http::message::query::{
        bdd::{
            functor_macros,
            functor_macros::{encode_functor_impl, impl_functor_for, impl_functor_for_multi},
            FunctorContext, FunctorEncoded,
        },
        concept::{AttributeTypeResponse, EntityTypeResponse, RelationTypeResponse, RoleTypeResponse},
        query_structure::StructureConstraint,
        AnalysedQueryResponse,
    };
    use crate::service::http::message::query::query_structure::AnalyzedFunctionResponse;

    pub fn encode_query_annotations_as_functor(analyzed: &AnalysedQueryResponse) -> (String, Vec<String>) {
        let query = PipelineAnnotationsToEncode.encode_as_functor(&FunctorContext { pipeline: &analyzed.query });
        let preamble = analyzed.preamble.iter().map(|func| {
            let AnalyzedFunctionResponse { body, argument_annotations, return_annotations, ..} = func;
            encode_functor_impl!(
                &FunctorContext { pipeline: &body },
                Function { argument_annotations, return_annotations, &PipelineAnnotationsToEncode, }
            )
        })
        .collect();
        (query, preamble)
    }

    pub fn encode_fetch_annotations_as_functor(analyzed: &AnalysedQueryResponse) -> String {
        analyzed.fetch.encode_as_functor(&FunctorContext { pipeline: &analyzed.query })
    }

    impl FunctorEncoded for PipelineAnnotationsToEncode {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            let encoded_stages = context.pipeline.stages.iter().map(|stage| {
                match stage {
                    QueryStructureStage::Match { block } => {
                        let block = &BlockAnnotationToEncode(block.as_u32() as usize);
                        encode_functor_impl!(context, Match { block, })
                    }
                    QueryStructureStage::Insert { block } => {
                        let block = &BlockAnnotationToEncode(block.as_u32() as usize);
                        encode_functor_impl!(context, Insert { block, })
                    }
                    QueryStructureStage::Delete { block, .. } => {
                        let block = &BlockAnnotationToEncode(block.as_u32() as usize);
                        encode_functor_impl!(context, Delete { block, })
                    }
                    QueryStructureStage::Put { block } => {
                        let block = &BlockAnnotationToEncode(block.as_u32() as usize);
                        encode_functor_impl!(context, Put { block, })
                    }
                    QueryStructureStage::Update { block } => {
                        let block = &BlockAnnotationToEncode(block.as_u32() as usize);
                        encode_functor_impl!(context, Update { block, })
                    }
                    QueryStructureStage::Select { .. } => encode_functor_impl!(context, Select {}),
                    QueryStructureStage::Sort { .. } => encode_functor_impl!(context, Sort {}),
                    QueryStructureStage::Offset { .. } => encode_functor_impl!(context, Offset {}),
                    QueryStructureStage::Limit { .. } => encode_functor_impl!(context, Limit {}),
                    QueryStructureStage::Require { .. } => encode_functor_impl!(context, Require {}),
                    QueryStructureStage::Distinct => encode_functor_impl!(context, Select {}),
                    QueryStructureStage::Reduce { .. } => encode_functor_impl!(context, Reduce {}),
                }
            }).collect::<Vec<_>>();
            encode_functor_impl!(context, Pipeline { &encoded_stages, }) // Not ideal to encode the elements again
        }
    }

    impl_functor_for!(enum FunctionReturnAnnotationsResponse [ Single { annotations, } | Stream { annotations, } | ]);
    struct PipelineAnnotationsToEncode;
    struct BlockAnnotationToEncode(usize);
    impl From<QueryStructureConjunctionID> for BlockAnnotationToEncode {
        fn from(value: QueryStructureConjunctionID) -> Self {
            Self(value.as_u32() as usize)
        }
    }

    enum SubBlockAnnotation {
        Or { branches: Vec<BlockAnnotationToEncode> },
        Not { conjunction: BlockAnnotationToEncode },
        Try { conjunction: BlockAnnotationToEncode },
    }

    impl FunctorEncoded for BlockAnnotationToEncode {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            let trunk = &context.pipeline.conjunctions[self.0].annotations;
            let subpatterns = context.pipeline.conjunctions[self.0].constraints
                .iter()
                .filter_map(|c| match &c.constraint {
                    StructureConstraint::Or { branches } => {
                        Some(SubBlockAnnotation::Or { branches: branches.iter().copied().map_into().collect() })
                    }
                    StructureConstraint::Not { conjunction } => {
                        Some(SubBlockAnnotation::Not { conjunction: (*conjunction).into() })
                    }
                    StructureConstraint::Try { conjunction } => {
                        Some(SubBlockAnnotation::Try { conjunction: (*conjunction).into() })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            encode_functor_impl!(context, And { trunk, &subpatterns, })
        }
    }

    impl_functor_for!(enum SubBlockAnnotation [ Or { branches, } | Not { conjunction, } | Try { conjunction, } | ]);
    impl_functor_for!(enum TypeAnnotationResponse [ Thing { annotations, } | Type { annotations, } | Value { value_types, } | ]);
    impl_functor_for_multi!(|self, context| [
        ConjunctionAnnotationsResponse => { self.variable_annotations.encode_as_functor(context) }
        SingleTypeAnnotationResponse =>  { self.label().to_owned().encode_as_functor(context) }
        VariableAnnotationsResponse => {
            debug_assert!(!self.is_optional); // Still has to be implemented
            self.annotations.encode_as_functor(context)
        }
    ]);

    impl FunctorEncoded for FetchStructureAnnotationsResponse {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            match self {
                FetchStructureAnnotationsResponse::Value { value_types } => value_types.encode_as_functor(context),
                FetchStructureAnnotationsResponse::Object { possible_fields } => {
                    let as_map =
                        possible_fields.iter().cloned().map(|kv| (kv.key, kv.value)).collect::<HashMap<_, _>>();
                    as_map.encode_as_functor(context)
                }
                FetchStructureAnnotationsResponse::List { elements } => {
                    encode_functor_impl!(context, List { elements.as_ref(), })
                }
            }
        }
    }
}
