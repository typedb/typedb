/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use ::concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use axum::response::{IntoResponse, Response};
use compiler::{
    annotation::function::FunctionParameterAnnotation,
    query_structure::{PipelineStructureAnnotations, StructureVariableId},
};
use http::StatusCode;
use options::QueryOptions;
use query::query_manager::{
    FetchObjectStructureAnnotations, FetchStructureAnnotations, FunctionStructureAnnotations, QueryStructureAnnotations,
};
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use storage::snapshot::ReadableSnapshot;
use tracing::Value;

use crate::service::{
    http::{
        message::{
            body::JsonBody,
            query::{
                concept::{encode_type_concept, encode_value_type},
                query_structure::PipelineStructureResponse,
            },
            transaction::TransactionOpenPayload,
        },
        transaction_service::QueryAnswer,
    },
    AnswerType, QueryType,
};

pub mod concept;
pub mod document;
pub mod query_structure;
pub mod row;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryOptionsPayload {
    pub include_instance_types: Option<bool>,
    pub answer_count_limit: Option<u64>,
}

impl Default for QueryOptionsPayload {
    fn default() -> Self {
        Self { include_instance_types: None, answer_count_limit: None }
    }
}

impl Into<QueryOptions> for QueryOptionsPayload {
    fn into(self) -> QueryOptions {
        QueryOptions {
            include_instance_types: self.include_instance_types.unwrap_or(DEFAULT_INCLUDE_INSTANCE_TYPES),
            answer_count_limit: self
                .answer_count_limit
                .map(|option| Some(option as usize))
                .unwrap_or(DEFAULT_ANSWER_COUNT_LIMIT_HTTP),
            prefetch_size: DEFAULT_PREFETCH_SIZE as usize,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionQueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
    pub commit: Option<bool>,

    #[serde(flatten)]
    pub transaction_open_payload: TransactionOpenPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryAnswerResponse {
    pub query_type: QueryType,
    pub answer_type: AnswerType,
    pub answers: Option<Vec<serde_json::Value>>,
    pub query: Option<PipelineStructureResponse>,
    pub warning: Option<String>,
}

pub(crate) fn encode_query_ok_answer(query_type: QueryType) -> QueryAnswerResponse {
    QueryAnswerResponse { answer_type: AnswerType::Ok, query_type, answers: None, query: None, warning: None }
}

pub(crate) fn encode_query_rows_answer(
    query_type: QueryType,
    rows: Vec<serde_json::Value>,
    pipeline_structure: Option<PipelineStructureResponse>,
    warning: Option<String>,
) -> QueryAnswerResponse {
    QueryAnswerResponse {
        answer_type: AnswerType::ConceptRows,
        query_type,
        answers: Some(rows),
        query: pipeline_structure,
        warning,
    }
}

pub(crate) fn encode_query_documents_answer(
    query_type: QueryType,
    documents: Vec<serde_json::Value>,
    warning: Option<String>,
) -> QueryAnswerResponse {
    QueryAnswerResponse {
        answer_type: AnswerType::ConceptDocuments,
        answers: Some(documents),
        query_type,
        query: None,
        warning,
    }
}

impl IntoResponse for QueryAnswer {
    fn into_response(self) -> Response {
        let code = self.status_code();
        let body = match self {
            QueryAnswer::ResOk(query_type) => JsonBody(encode_query_ok_answer(query_type)),
            QueryAnswer::ResRows((query_type, rows, pipeline_structure, warning)) => {
                JsonBody(encode_query_rows_answer(
                    query_type,
                    rows,
                    pipeline_structure,
                    warning.map(|warning| warning.to_string()),
                ))
            }
            QueryAnswer::ResDocuments((query_type, documents, warning)) => JsonBody(encode_query_documents_answer(
                query_type,
                documents,
                warning.map(|warning| warning.to_string()),
            )),
        };
        (code, body).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum TypeAnnotationResponse {
    Concept { annotations: Vec<serde_json::Value> },
    Value { value_types: Vec<serde_json::Value> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableAnnotationsByBlockResponse {
    variable_annotations: HashMap<StructureVariableId, TypeAnnotationResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnalysedPipelineResponse {
    annotations_by_block: Vec<VariableAnnotationsByBlockResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnalysedFunctionResponse {
    arguments: Vec<TypeAnnotationResponse>,
    returned: Vec<TypeAnnotationResponse>,
    pipeline: Option<AnalysedPipelineResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
enum AnalysedFetchObjectResponse {
    Leaf(Vec<String>), // Value types encoded as string
    Object(HashMap<String, AnalysedFetchObjectResponse>),
}

#[derive(Debug, Serialize, Deserialize)]
struct AnalysedQueryAnnotationsResponse {
    #[serde(skip_serializing)] // TODO: Include when we sort out the function structure
    preamble: Vec<AnalysedFunctionResponse>,

    pipeline: Option<AnalysedPipelineResponse>,
    fetch: Option<HashMap<String, AnalysedFetchObjectResponse>>,
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
    analysed_query: QueryStructureAnnotations,
) -> Result<AnalysedQueryResponse, Box<ConceptReadError>> {
    let QueryStructureAnnotations { pipeline, preamble, fetch } = analysed_query;
    let preamble = preamble
        .into_iter()
        .map(|function| encode_function_structure_annotations(snapshot, type_manager, function))
        .collect::<Result<Vec<_>, _>>()?;
    let pipeline = pipeline
        .map(|pipeline_annotations| encode_pipeline_structure_annotations(snapshot, type_manager, pipeline_annotations))
        .transpose()?;
    let fetch = fetch.map(|fetch| encode_analysed_fetch(snapshot, type_manager, fetch)).transpose()?;
    let annotations = AnalysedQueryAnnotationsResponse { preamble, pipeline, fetch };
    Ok(AnalysedQueryResponse { annotations })
}

fn encode_pipeline_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline: PipelineStructureAnnotations,
) -> Result<AnalysedPipelineResponse, Box<ConceptReadError>> {
    let mut annotations_by_block = Vec::new();
    pipeline.iter().try_for_each(|(block_id, var_annotations)| {
        let block_id = block_id.0 as usize;
        if annotations_by_block.len() <= block_id {
            annotations_by_block.resize_with(block_id + 1, || VariableAnnotationsByBlockResponse {
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
        annotations_by_block[block_id] = VariableAnnotationsByBlockResponse { variable_annotations };
        Ok::<_, Box<ConceptReadError>>(())
    })?;
    Ok(AnalysedPipelineResponse { annotations_by_block })
}

pub(crate) fn encode_function_structure_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    analysed_function: FunctionStructureAnnotations,
) -> Result<AnalysedFunctionResponse, Box<ConceptReadError>> {
    let FunctionStructureAnnotations { pipeline, signature: sig } = analysed_function;
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
    Ok(AnalysedFunctionResponse { arguments, returned, pipeline })
}

fn encode_analysed_fetch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    analysed_fetch_object: FetchStructureAnnotations,
) -> Result<HashMap<String, AnalysedFetchObjectResponse>, Box<ConceptReadError>> {
    analysed_fetch_object
        .into_iter()
        .map(|(key, object)| {
            // TODO: We don't encode the pipeline anywhere
            let encoded = match object {
                FetchObjectStructureAnnotations::Function { pipeline: _, returned: leaf }
                | FetchObjectStructureAnnotations::Leaf(leaf) => {
                    let value_types = leaf
                        .into_iter()
                        .map(|v| encode_value_type(v, snapshot, type_manager))
                        .collect::<Result<Vec<_>, _>>()?;
                    AnalysedFetchObjectResponse::Leaf(value_types)
                }
                FetchObjectStructureAnnotations::SubFetch { pipeline: _, fetch: object }
                | FetchObjectStructureAnnotations::Object(object) => {
                    AnalysedFetchObjectResponse::Object(encode_analysed_fetch(snapshot, type_manager, object)?)
                }
            };
            Ok((key, encoded))
        })
        .collect::<Result<HashMap<_, _>, _>>()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysedQueryResponse {
    // structure: QueryStructureResponse, // TODO:
    annotations: AnalysedQueryAnnotationsResponse,
}

impl IntoResponse for AnalysedQueryResponse {
    fn into_response(self) -> Response {
        let code = StatusCode::OK;
        let body = JsonBody(self);
        (code, body).into_response()
    }
}
