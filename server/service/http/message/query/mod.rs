/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ::concept::{
    error::ConceptDecodeError,
    thing::{ThingAPI, attribute::Attribute, entity::Entity, relation::Relation},
};
use answer::{Thing, variable_value::VariableValue};
use axum::response::{IntoResponse, Response};
use bytes::util::HexBytesFormatter;
use compiler::VariablePosition;
use encoding::graph::thing::{ThingVertex, vertex_attribute::AttributeVertex, vertex_object::ObjectVertex};
use executor::batch::Batch;
use ir::translation::parse_iid;
use options::QueryOptions;
use query::query_manager::GivenRows;
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_INCLUDE_STRUCTURE_HTTP,
    DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};

use crate::service::{
    AnswerType, QueryType,
    http::{
        error::HttpServiceError,
        message::{
            analyze::structure::AnalyzedPipelineResponse,
            body::JsonBody,
            query::concept::{AttributeResponse, EntityResponse, RelationResponse, ValueResponse, decode_value},
            transaction::TransactionOpenPayload,
        },
        transaction_service::QueryAnswer,
    },
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
    pub given_rows: Option<GivenRowsPayload>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
    pub given_rows: Option<GivenRowsPayload>,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GivenRowsPayload {
    pub variables: Vec<String>,
    pub rows: Vec<Vec<GivenEntryPayload>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum GivenEntryPayload {
    #[default]
    Empty,
    Entity(EntityResponse),
    Relation(RelationResponse),
    Attribute(AttributeResponse),
    Value(ValueResponse),
}

macro_rules! concept_decode_error {
    ($variant:ident, $iid:ident) => {
        HttpServiceError::ConceptDecode {
            typedb_source: Box::new(ConceptDecodeError::$variant {
                iid: HexBytesFormatter::borrowed($iid.as_ref()).into_owned(),
            }),
        }
    };
}

impl TryInto<GivenRows> for GivenRowsPayload {
    type Error = HttpServiceError;
    fn try_into(self) -> Result<GivenRows, Self::Error> {
        let variables = self.variables;
        let rows = self.rows;
        let len = rows.len();
        let width = rows.first().map(|row| row.len() as u32).unwrap_or(0);
        let mut batch = Batch::new(width, len);
        rows.into_iter().try_for_each(|row| {
            batch.append(|mut write_to| {
                row.into_iter().enumerate().try_for_each(|(column, entry)| {
                    let value: VariableValue<'static> = match entry {
                        GivenEntryPayload::Empty => VariableValue::None,
                        GivenEntryPayload::Value(value) => VariableValue::Value(decode_value(value)?),
                        GivenEntryPayload::Entity(entity) => {
                            let iid = parse_iid(entity.iid.as_str()).map_err(|_: ()| {
                                HttpServiceError::InvalidIIDFormatForGivenEntry { iid: entity.iid.clone().to_owned() }
                            })?;
                            let vertex = ObjectVertex::try_decode(&iid)
                                .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsEntity, iid))?;
                            if !vertex.is_entity() {
                                return Err(concept_decode_error!(CouldNotDecodeIIDAsEntity, iid));
                            }
                            VariableValue::Thing(Thing::Entity(Entity::new(vertex)))
                        }
                        GivenEntryPayload::Relation(relation) => {
                            let iid = parse_iid(relation.iid.as_str()).map_err(|_: ()| {
                                HttpServiceError::InvalidIIDFormatForGivenEntry { iid: relation.iid.clone().to_owned() }
                            })?;
                            let vertex = ObjectVertex::try_decode(&iid)
                                .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsRelation, iid))?;
                            if !vertex.is_relation() {
                                return Err(concept_decode_error!(CouldNotDecodeIIDAsRelation, iid));
                            }
                            VariableValue::Thing(Thing::Relation(Relation::new(vertex)))
                        }
                        GivenEntryPayload::Attribute(attribute) => {
                            let iid = parse_iid(attribute.iid.as_str()).map_err(|_: ()| {
                                HttpServiceError::InvalidIIDFormatForGivenEntry {
                                    iid: attribute.iid.clone().to_owned(),
                                }
                            })?;
                            let vertex = AttributeVertex::try_decode(&iid)
                                .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsAttribute, iid))?;
                            VariableValue::Thing(Thing::Attribute(Attribute::new(vertex)))
                        }
                    };
                    write_to.set(VariablePosition::new(column as u32), value);
                    Ok::<_, HttpServiceError>(())
                })
            })
        })?;
        Ok(GivenRows { variables, batch })
    }
}
