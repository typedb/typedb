/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use axum::response::{IntoResponse, Response};
use options::QueryOptions;
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_INCLUDE_STRUCTURE_HTTP,
    DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};

use crate::service::{
    http::{
        message::{analyze::structure::AnalyzedPipelineResponse, body::JsonBody, transaction::TransactionOpenPayload},
        transaction_service::QueryAnswer,
    },
    AnswerType, QueryType,
};

pub mod concept;
pub mod document;
pub mod row;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryOptionsPayload {
    pub include_instance_types: Option<bool>,
    pub answer_count_limit: Option<u64>,
    pub include_query_structure: Option<bool>,
}

impl Default for QueryOptionsPayload {
    fn default() -> Self {
        Self { include_instance_types: None, answer_count_limit: None, include_query_structure: None }
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
            include_query_structure: self.include_query_structure.unwrap_or(DEFAULT_INCLUDE_STRUCTURE_HTTP),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionQueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    pub query: Option<AnalyzedPipelineResponse>,
    pub warning: Option<String>,
}

pub(crate) fn encode_query_ok_answer(query_type: QueryType) -> QueryAnswerResponse {
    QueryAnswerResponse { answer_type: AnswerType::Ok, query_type, answers: None, query: None, warning: None }
}

pub(crate) fn encode_query_rows_answer(
    query_type: QueryType,
    rows: Vec<serde_json::Value>,
    pipeline_structure: Option<AnalyzedPipelineResponse>,
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
