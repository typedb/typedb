/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use options::QueryOptions;
use query::query_manager::{AnalysedFetchObject, AnalysedQuery};
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::Value;
use ::concept::error::ConceptReadError;
use ::concept::type_::type_manager::TypeManager;
use compiler::annotation::function::{AnnotatedFunctionSignature, FunctionParameterAnnotation};
use storage::snapshot::ReadableSnapshot;

use crate::service::{
    http::{
        message::{
            body::JsonBody, query::query_structure::QueryStructureResponse, transaction::TransactionOpenPayload,
        },
        transaction_service::QueryAnswer,
    },
    AnswerType, QueryType,
};
use crate::service::http::message::query::concept::{encode_type_concept, encode_value_type};
use crate::service::http::message::query::query_structure::encode_query_structure;

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
    pub query: Option<QueryStructureResponse>,
    pub warning: Option<String>,
}

pub(crate) fn encode_query_ok_answer(query_type: QueryType) -> QueryAnswerResponse {
    QueryAnswerResponse { answer_type: AnswerType::Ok, query_type, answers: None, query: None, warning: None }
}

pub(crate) fn encode_query_rows_answer(
    query_type: QueryType,
    rows: Vec<serde_json::Value>,
    query_structure: Option<QueryStructureResponse>,
    warning: Option<String>,
) -> QueryAnswerResponse {
    QueryAnswerResponse {
        answer_type: AnswerType::ConceptRows,
        query_type,
        answers: Some(rows),
        query: query_structure,
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
            QueryAnswer::ResRows((query_type, rows, query_structure, warning)) => JsonBody(encode_query_rows_answer(
                query_type,
                rows,
                query_structure,
                warning.map(|warning| warning.to_string()),
            )),
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
#[serde(rename_all = "camelCase")]
enum TypeAnnotationResponse {
    Concept { annotations: Vec<serde_json::Value> },
    Value { value_types: Vec<serde_json::Value> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionSignatureResponse {
    arguments: Vec<TypeAnnotationResponse>,
    returned: Vec<TypeAnnotationResponse>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum AnalysedFetchObjectResponse {
    Leaf(TypeAnnotationResponse),
    Object(HashMap<String, AnalysedFetchObjectResponse>),
    Pipeline(AnalysedQueryAnswer)
}

fn encode_type_annotations(
    snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, annotation: FunctionParameterAnnotation
) -> Result<TypeAnnotationResponse, Box<ConceptReadError>> {
    Ok(match annotation {
        FunctionParameterAnnotation::Concept(types) => {
            TypeAnnotationResponse::Concept {
                annotations: types.into_iter().map(|type_| encode_type_concept(&type_, snapshot, type_manager)).collect::<Result<Vec<_>,_>>()?
            }
        }
        FunctionParameterAnnotation::Value(v) => {
            TypeAnnotationResponse::Value { value_types: vec![json!(encode_value_type(v, snapshot, type_manager)?)] }
        },
    })
}

pub(crate) fn encode_analysed_query(
    snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, analysed_query: AnalysedQuery
) -> Result<AnalysedQueryAnswer, Box<ConceptReadError>> {
    let AnalysedQuery { pipeline, signature, fetch } = analysed_query;
    let signature = signature.map(|sig| {
        Ok::<_, Box<ConceptReadError>>(FunctionSignatureResponse {
            arguments: sig.arguments.into_iter().map(|arg| encode_type_annotations(snapshot, type_manager, arg)).collect::<Result<Vec<_>, _>>()?,
            returned: sig.returned.into_iter().map(|arg| encode_type_annotations(snapshot, type_manager, arg)).collect::<Result<Vec<_>, _>>()?,
        })
    }).transpose()?;
    let pipeline = pipeline.map(|query_structure| encode_query_structure(snapshot, type_manager, &query_structure)).transpose()?;
    let fetch = fetch.map(|fetch| encode_analysed_fetch(snapshot, type_manager, fetch)).transpose()?;
    Ok(AnalysedQueryAnswer { signature, pipeline, fetch })
}

fn encode_analysed_fetch(
    snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, analysed_fetch_object: HashMap<String, AnalysedFetchObject>
) -> Result<HashMap<String, AnalysedFetchObjectResponse>, Box<ConceptReadError>> {
    analysed_fetch_object.into_iter().map(|(key, object)| {
        let encoded = match object {
            AnalysedFetchObject::Leaf(leaf) => {
                let value_types = leaf.into_iter().map(|v| json!(v.to_string())).collect();
                AnalysedFetchObjectResponse::Leaf(TypeAnnotationResponse::Value { value_types })
            },
            AnalysedFetchObject::Object(object) => AnalysedFetchObjectResponse::Object(encode_analysed_fetch(snapshot, type_manager, object)?),
            AnalysedFetchObject::Pipeline(pipeline) => AnalysedFetchObjectResponse::Pipeline(encode_analysed_query(snapshot, type_manager, pipeline)?),
        };
        Ok((key, encoded))
    }).collect::<Result<HashMap<_, _>, _>>()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysedQueryAnswer {
    signature: Option<FunctionSignatureResponse>,
    pipeline: Option<QueryStructureResponse>,
    fetch: Option<HashMap<String, AnalysedFetchObjectResponse>>,
}

impl IntoResponse for AnalysedQueryAnswer {
    fn into_response(self) -> Response {
        let code = StatusCode::OK;
        let body = JsonBody(self);
        (code, body).into_response()
    }
}
