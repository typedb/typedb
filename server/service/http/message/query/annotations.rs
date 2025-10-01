/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::Type;
use compiler::{
    annotation::function::FunctionParameterAnnotation,
    query_structure::{PipelineStructureAnnotations, PipelineVariableAnnotation, StructureVariableId},
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use query::analyse::{
    AnalysedQuery, FetchObjectStructureAnnotations, FetchStructureAnnotations, FunctionStructureAnnotations,
    QueryStructureAnnotations,
};
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::{
    concept::{
        encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_value_type,
        AttributeTypeResponse, EntityTypeResponse, RelationTypeResponse, RoleTypeResponse,
    },
    query_structure::encode_query_structure,
    AnalysedQueryResponse,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub(crate) enum SingleTypeAnnotationResponse {
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
#[serde(rename_all = "camelCase", tag = "tag")]
pub(crate) enum TypeAnnotationResponse {
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
pub(crate) struct VariableAnnotationsByConjunctionResponse {
    pub(crate) variable_annotations: HashMap<StructureVariableId, TypeAnnotationResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PipelineStructureAnnotationsResponse {
    pub(crate) annotations_by_conjunction: Vec<VariableAnnotationsByConjunctionResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(crate) enum FunctionReturnAnnotationsResponse {
    Single { annotations: Vec<TypeAnnotationResponse> },
    Stream { annotations: Vec<TypeAnnotationResponse> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FunctionStructureAnnotationsResponse {
    pub(crate) arguments: Vec<TypeAnnotationResponse>,
    pub(crate) returns: FunctionReturnAnnotationsResponse,
    pub(crate) body: PipelineStructureAnnotationsResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(crate) enum FetchStructureAnnotationsResponse {
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
    pub(crate) key: String,
    #[serde(flatten)]
    pub(crate) value: FetchStructureAnnotationsResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryStructureAnnotationsResponse {
    pub(crate) preamble: Vec<FunctionStructureAnnotationsResponse>,
    pub(crate) query: PipelineStructureAnnotationsResponse,
    pub(crate) fetch: Option<FetchStructureAnnotationsResponse>, // Will always be the 'Object' variant
}

fn encode_function_parameter_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotation: FunctionParameterAnnotation,
) -> Result<TypeAnnotationResponse, Box<ConceptReadError>> {
    Ok(match annotation {
        FunctionParameterAnnotation::Concept(types) => {
            TypeAnnotationResponse::Thing { annotations: encode_type_annotation_vec(snapshot, type_manager, types.iter())?, }
        },
        FunctionParameterAnnotation::Value(v) => {
            TypeAnnotationResponse::Value { value_types: vec![encode_value_type(v, snapshot, type_manager)?] }
        }
    })
}

fn encode_variable_type_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotation: &PipelineVariableAnnotation,
) -> Result<TypeAnnotationResponse, Box<ConceptReadError>> {
    Ok(match annotation {
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
    })
}

fn encode_type_annotation_vec<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    types: impl Iterator<Item = &'a Type>,
) -> Result<Vec<SingleTypeAnnotationResponse>, Box<ConceptReadError>> {
    let mut encoded = types.map(|t| encode_type_annotation(snapshot, type_manager, t)).collect::<Result<Vec<_>, _>>()?;
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

pub fn encode_query_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    analysed_query: AnalysedQuery,
) -> Result<AnalysedQueryResponse, Box<ConceptReadError>> {
    let AnalysedQuery { structure, annotations: analysed_query_annotations } = analysed_query;
    let QueryStructureAnnotations { query: pipeline, preamble, fetch } = analysed_query_annotations;
    let preamble = preamble
        .into_iter()
        .map(|function| encode_function_structure_annotations(snapshot, type_manager, function))
        .collect::<Result<Vec<_>, _>>()?;
    let pipeline = encode_pipeline_structure_annotations(snapshot, type_manager, pipeline)?;
    let fetch = fetch
        .map(|fetch| {
            encode_fetch_structure_annotations(snapshot, type_manager, fetch)
                .map(|fields| FetchStructureAnnotationsResponse::Object { possible_fields: fields })
        })
        .transpose()?;
    let annotations = QueryStructureAnnotationsResponse { preamble, query: pipeline, fetch };
    let structure = encode_query_structure(snapshot, type_manager, structure)?;
    Ok(AnalysedQueryResponse { structure, annotations })
}

fn encode_pipeline_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure_annotations: PipelineStructureAnnotations,
) -> Result<PipelineStructureAnnotationsResponse, Box<ConceptReadError>> {
    let mut annotations_by_conjunction = Vec::new();
    pipeline_structure_annotations.iter().try_for_each(|(conj_id, var_annotations)| {
        let conj_id = conj_id.0 as usize;
        if annotations_by_conjunction.len() <= conj_id {
            annotations_by_conjunction.resize_with(conj_id + 1, || VariableAnnotationsByConjunctionResponse {
                variable_annotations: HashMap::new(),
            });
        }
        let variable_annotations = var_annotations
            .into_iter()
            .map(|(var_id, annotations)| {
                Ok::<_, Box<ConceptReadError>>((
                    var_id.clone(),
                    encode_variable_type_annotations(snapshot, type_manager, annotations)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        annotations_by_conjunction[conj_id] = VariableAnnotationsByConjunctionResponse { variable_annotations };
        Ok::<_, Box<ConceptReadError>>(())
    })?;
    Ok(PipelineStructureAnnotationsResponse { annotations_by_conjunction })
}

pub(crate) fn encode_function_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    function_structure_annotations: FunctionStructureAnnotations,
) -> Result<FunctionStructureAnnotationsResponse, Box<ConceptReadError>> {
    let FunctionStructureAnnotations { body: pipeline, signature: sig } = function_structure_annotations;
    let arguments = sig
        .arguments
        .into_iter()
        .map(|arg| encode_function_parameter_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let return_types = sig
        .returns
        .into_iter()
        .map(|arg| encode_function_parameter_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let body = encode_pipeline_structure_annotations(snapshot, type_manager, pipeline)?;
    let returns = match sig.is_stream {
        true => FunctionReturnAnnotationsResponse::Stream { annotations: return_types },
        false => FunctionReturnAnnotationsResponse::Single { annotations: return_types },
    };
    Ok(FunctionStructureAnnotationsResponse { arguments, returns, body })
}

fn encode_fetch_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    fetch_structure_annotations: FetchStructureAnnotations,
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
    fetch_object_structure_annotations: FetchObjectStructureAnnotations,
) -> Result<FetchStructureAnnotationsResponse, Box<ConceptReadError>> {
    // TODO: We don't encode the pipeline anywhere
    let encoded = match fetch_object_structure_annotations {
        FetchObjectStructureAnnotations::Leaf(leaf) => {
            let value_types = leaf
                .into_iter()
                .map(|v| encode_value_type(v, snapshot, type_manager))
                .collect::<Result<Vec<_>, _>>()?;
            FetchStructureAnnotationsResponse::Value { value_types }
        }
        FetchObjectStructureAnnotations::Object(object) => FetchStructureAnnotationsResponse::Object {
            possible_fields: encode_fetch_structure_annotations(snapshot, type_manager, object)?,
        },
        FetchObjectStructureAnnotations::List(boxed_list_annotations) => {
            let elements = encode_fetch_object_structure_annotations(snapshot, type_manager, *boxed_list_annotations)?;
            FetchStructureAnnotationsResponse::List { elements: Box::new(elements) }
        }
    };
    Ok(encoded)
}
