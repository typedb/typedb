/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use compiler::{
    annotation::function::FunctionParameterAnnotation,
    query_structure::{PipelineStructureAnnotations, StructureVariableId},
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use query::analyse::{
    AnalysedQuery, FetchObjectStructureAnnotations, FetchStructureAnnotations, FunctionStructureAnnotations,
    QueryStructureAnnotations,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::{
    concept::{encode_type_concept, encode_value_type},
    query_structure::encode_query_structure,
    AnalysedQueryResponse,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum TypeAnnotationResponse {
    Concept { annotations: Vec<serde_json::Value> },
    #[serde(rename_all = "camelCase")]
    Value { value_types: Vec<serde_json::Value> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableAnnotationsByConjunctionResponse {
    variable_annotations: HashMap<StructureVariableId, TypeAnnotationResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineStructureAnnotationsResponse {
    annotations_by_conjunction: Vec<VariableAnnotationsByConjunctionResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionStructureAnnotationsResponse {
    arguments: Vec<TypeAnnotationResponse>,
    returned: Vec<TypeAnnotationResponse>,
    body: Option<PipelineStructureAnnotationsResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum FetchStructureAnnotationsResponse {
    Value { types: Vec<String> }, // Value types encoded as string
    Object { fields: Vec<FetchStructureFieldAnnotationsResponse> },
    List { elements: Box<FetchStructureAnnotationsResponse> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchStructureFieldAnnotationsResponse {
    key: String,
    #[serde(flatten)]
    value: FetchStructureAnnotationsResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryStructureAnnotationsResponse {
    preamble: Vec<FunctionStructureAnnotationsResponse>,
    query: Option<PipelineStructureAnnotationsResponse>,
    fetch: Option<FetchStructureAnnotationsResponse>, // Will always be the 'Object' variant
}

fn encode_type_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotation: FunctionParameterAnnotation,
) -> Result<TypeAnnotationResponse, Box<ConceptReadError>> {
    Ok(match annotation {
        FunctionParameterAnnotation::Concept(types) => TypeAnnotationResponse::Concept {
            annotations: types
                .into_iter()
                .map(|type_| encode_type_concept(&type_, snapshot, type_manager))
                .collect::<Result<Vec<_>, _>>()?,
        },
        FunctionParameterAnnotation::Value(v) => {
            TypeAnnotationResponse::Value { value_types: vec![json!(encode_value_type(v, snapshot, type_manager)?)] }
        }
    })
}

pub(crate) fn encode_query_structure_annotations(
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
    let pipeline = pipeline
        .map(|pipeline_annotations| encode_pipeline_structure_annotations(snapshot, type_manager, pipeline_annotations))
        .transpose()?;
    let fetch = fetch
        .map(|fetch| {
            encode_fetch_structure_annotations(snapshot, type_manager, fetch)
                .map(|fields| FetchStructureAnnotationsResponse::Object { fields })
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
                    encode_type_annotations(snapshot, type_manager, annotations.clone())?,
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
        .map(|arg| encode_type_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let returned = sig
        .returned
        .into_iter()
        .map(|arg| encode_type_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let pipeline = pipeline
        .map(|pipeline_annotations| encode_pipeline_structure_annotations(snapshot, type_manager, pipeline_annotations))
        .transpose()?;
    Ok(FunctionStructureAnnotationsResponse { arguments, returned, body: pipeline })
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
            let types = leaf
                .into_iter()
                .map(|v| encode_value_type(v, snapshot, type_manager))
                .collect::<Result<Vec<_>, _>>()?;
            FetchStructureAnnotationsResponse::Value { types }
        }
        FetchObjectStructureAnnotations::Object(object) => FetchStructureAnnotationsResponse::Object {
            fields: encode_fetch_structure_annotations(snapshot, type_manager, object)?,
        },
        FetchObjectStructureAnnotations::List(boxed_list_annotations) => {
            let elements = encode_fetch_object_structure_annotations(snapshot, type_manager, *boxed_list_annotations)?;
            FetchStructureAnnotationsResponse::List { elements: Box::new(elements) }
        }
    };
    Ok(encoded)
}
