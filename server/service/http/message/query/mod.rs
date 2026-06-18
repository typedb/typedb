/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::{BTreeMap, BTreeSet};

use ::concept::{
    error::ConceptDecodeError,
    thing::{ThingAPI, attribute::Attribute, entity::Entity, relation::Relation},
};
use answer::{Thing, Type};
use axum::response::{IntoResponse, Response};
use bytes::util::HexBytesFormatter;
use clap::builder::TypedValueParser;
use compiler::annotation::function::FunctionParameterAnnotation;
use encoding::{
    graph::thing::{ThingVertex, vertex_attribute::AttributeVertex, vertex_object::ObjectVertex},
    value::{ValueEncodable, value::Value, value_type::ValueType},
};
use ir::translation::{literal::FromTypeQLLiteral, parse_iid};
use options::QueryOptions;
use query::given_rows::{GivenRowDecodeError, GivenRowEntry, GivenRows};
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_INCLUDE_STRUCTURE_HTTP,
    DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};
use typeql::parse_value;

use crate::service::{
    AnswerType, QueryType,
    http::{
        message::{
            analyze::structure::AnalyzedPipelineResponse,
            body::JsonBody,
            query::concept::{AttributeResponse, EntityResponse, RelationResponse, ValueResponse},
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
pub struct GivenRowsPayload(pub Vec<BTreeMap<String, Option<GivenEntryPayload>>>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum TaggedEntryPayload {
    Entity(EntityResponse),
    Relation(RelationResponse),
    Attribute(AttributeResponse),
    Value(ValueResponse),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GivenEntryPayload {
    PlainString(String),
    PlainNumber(serde_json::Number),
    PlainBool(bool),
    Concept(TaggedEntryPayload),
}

macro_rules! concept_decode_error {
    ($variant:ident, $iid:ident) => {
        GivenRowDecodeError::ConceptDecode {
            typedb_source: Box::new(ConceptDecodeError::$variant {
                iid: HexBytesFormatter::borrowed($iid.as_ref()).into_owned(),
            }),
        }
    };
}

#[derive(Debug)]
pub struct GivenRowsHttp {
    pub variables: Vec<String>,
    pub rows: Vec<Vec<Option<GivenEntryPayload>>>,
}

impl std::convert::From<GivenRowsPayload> for GivenRowsHttp {
    fn from(value: GivenRowsPayload) -> Self {
        let variables_as_set = value.0.iter().flat_map(|v| v.keys().cloned()).collect::<BTreeSet<String>>();
        let variables = Vec::from_iter(variables_as_set.into_iter());
        let variables_ref = &variables;
        let rows = value
            .0
            .into_iter()
            .map(|mut as_map| variables_ref.iter().map(move |var| as_map.remove(var).flatten()).collect())
            .collect();
        Self { variables, rows }
    }
}

impl GivenRows for GivenRowsHttp {
    type Item = Option<GivenEntryPayload>;
    type Row = Vec<Option<GivenEntryPayload>>;

    fn variables(&self) -> &[String] {
        self.variables.as_slice()
    }

    fn decode(
        item: Self::Item,
        expected_type: &FunctionParameterAnnotation,
    ) -> Result<GivenRowEntry, GivenRowDecodeError> {
        if let Some(item) = item {
            match item {
                GivenEntryPayload::PlainNumber(value) => {
                    let FunctionParameterAnnotation::Value(expected_type) = expected_type else {
                        return Err(GivenRowDecodeError::ExpectedInstanceReceivedValue {});
                    };
                    let decoded_value = match expected_type {
                        ValueType::Integer => value.as_i64().map(Value::Integer),
                        ValueType::Double => value.as_f64().map(Value::Double),
                        _ => None,
                    }
                    .ok_or_else(|| GivenRowDecodeError::ValueTypeMismatch {
                        expected_type: expected_type.clone(),
                        actual_type: "json::number".to_owned(),
                        value: value.to_string(),
                    })?;
                    Ok(GivenRowEntry::Value(decoded_value))
                }
                GivenEntryPayload::PlainBool(value) => {
                    let FunctionParameterAnnotation::Value(expected_type) = expected_type else {
                        return Err(GivenRowDecodeError::ExpectedInstanceReceivedValue {});
                    };
                    match expected_type {
                        ValueType::Boolean => Ok(GivenRowEntry::Value(Value::Boolean(value))),
                        _ => Err(GivenRowDecodeError::ValueTypeMismatch {
                            expected_type: expected_type.clone(),
                            actual_type: "boolean".to_owned(),
                            value: value.to_string(),
                        }),
                    }
                }
                GivenEntryPayload::PlainString(value) => match expected_type {
                    FunctionParameterAnnotation::AnyConcept => unreachable!("Can't be"),
                    FunctionParameterAnnotation::Concept(expected_type) => decode_string_as_iid(value, expected_type),
                    FunctionParameterAnnotation::Value(expected_type) => decode_string_as_value(value, expected_type),
                },
                GivenEntryPayload::Concept(tagged) => match tagged {
                    TaggedEntryPayload::Value(value) => {
                        GivenRowEntry::try_cast_value_to(decode_value(value)?, expected_type)
                    }
                    TaggedEntryPayload::Entity(entity) => {
                        let iid = parse_iid(entity.iid.as_str()).map_err(|_: ()| {
                            GivenRowDecodeError::InvalidIIDFormatForGivenEntry { iid: entity.iid.clone().to_owned() }
                        })?;
                        let vertex = ObjectVertex::try_decode(&iid)
                            .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsEntity, iid))?;
                        if !vertex.is_entity() {
                            return Err(concept_decode_error!(CouldNotDecodeIIDAsEntity, iid));
                        }
                        Ok(GivenRowEntry::Thing(Thing::Entity(Entity::new(vertex))))
                    }
                    TaggedEntryPayload::Relation(relation) => {
                        let iid = parse_iid(relation.iid.as_str()).map_err(|_: ()| {
                            GivenRowDecodeError::InvalidIIDFormatForGivenEntry { iid: relation.iid.clone().to_owned() }
                        })?;
                        let vertex = ObjectVertex::try_decode(&iid)
                            .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsRelation, iid))?;
                        if !vertex.is_relation() {
                            return Err(concept_decode_error!(CouldNotDecodeIIDAsRelation, iid));
                        }
                        Ok(GivenRowEntry::Thing(Thing::Relation(Relation::new(vertex))))
                    }
                    TaggedEntryPayload::Attribute(attribute) => {
                        let iid = parse_iid(attribute.iid.as_str()).map_err(|_: ()| {
                            GivenRowDecodeError::InvalidIIDFormatForGivenEntry { iid: attribute.iid.clone().to_owned() }
                        })?;
                        let vertex = AttributeVertex::try_decode(&iid)
                            .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsAttribute, iid))?;
                        Ok(GivenRowEntry::Thing(Thing::Attribute(Attribute::new(vertex))))
                    }
                },
            }
        } else {
            Ok(GivenRowEntry::None)
        }
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn rows(self) -> impl Iterator<Item = Self::Row> {
        self.rows.into_iter()
    }

    fn iter_row(row: Self::Row) -> impl Iterator<Item = Self::Item> {
        row.into_iter()
    }
}

fn decode_string_as_iid(iid_string: String, _types: &BTreeSet<Type>) -> Result<GivenRowEntry, GivenRowDecodeError> {
    let iid = parse_iid(iid_string.as_str())
        .map_err(|_: ()| GivenRowDecodeError::InvalidIIDFormatForGivenEntry { iid: iid_string.clone() })?;
    let prefix = iid_string[0..4].to_lowercase();
    match prefix.as_str() {
        "0x1e" => {
            let vertex =
                ObjectVertex::try_decode(&*iid).ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsEntity, iid))?;
            Ok(GivenRowEntry::Thing(Thing::Entity(Entity::new(vertex))))
        }
        "0x1f" => {
            let vertex =
                ObjectVertex::try_decode(&*iid).ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsEntity, iid))?;
            Ok(GivenRowEntry::Thing(Thing::Relation(Relation::new(vertex))))
        }
        "0x20" => {
            let vertex = AttributeVertex::try_decode(&*iid)
                .ok_or_else(|| concept_decode_error!(CouldNotDecodeIIDAsEntity, iid))?;
            Ok(GivenRowEntry::Thing(Thing::Attribute(Attribute::new(vertex))))
        }
        _ => Err(GivenRowDecodeError::InvalidIIDFormatForGivenEntry { iid: iid_string }),
    }
}

fn decode_string_as_value(value: String, expected_type: &ValueType) -> Result<GivenRowEntry, GivenRowDecodeError> {
    let parsed_value = if expected_type == &ValueType::String {
        parse_value(format!("\"{value}\"").as_str())
    } else if expected_type == &ValueType::Decimal && !value.as_str().ends_with("dec") {
        parse_value(format!("{value}dec").as_str())
    } else {
        parse_value(value.as_str())
    }
    .map_err(|typedb_source| GivenRowDecodeError::ParsingValueFailedForGivenEntry {
        value: value.clone(),
        typedb_source,
    })?;
    let translated_value = Value::from_typeql_literal(&parsed_value, None).map_err(|typedb_source| {
        GivenRowDecodeError::TranslatingValueFailedForGivenEntry { value: value.clone(), typedb_source }
    })?;
    if &translated_value.value_type() == expected_type {
        Ok(GivenRowEntry::Value(translated_value))
    } else {
        Err(GivenRowDecodeError::ValueTypeMismatch {
            expected_type: expected_type.clone(),
            actual_type: translated_value.value_type().to_string(),
            value: value.to_string(),
        })
    }
}

pub fn decode_value(value: ValueResponse) -> Result<Value<'static>, GivenRowDecodeError> {
    fn decode(to_parse: &str) -> Result<Value<'static>, GivenRowDecodeError> {
        let parsed = typeql::parse_value(to_parse).map_err(|typedb_source| {
            GivenRowDecodeError::ParsingValueFailedForGivenEntry { value: to_parse.to_owned(), typedb_source }
        })?;
        Value::from_typeql_literal(&parsed, None).map_err(|typedb_source| {
            GivenRowDecodeError::TranslatingValueFailedForGivenEntry { value: to_parse.to_owned(), typedb_source }
        })
    }
    let as_str = value.value.to_string();
    match value.value_type.as_str() {
        "decimal" => decode(&format!("{}dec", &as_str[1..as_str.len() - 1])),
        "date" | "datetime" | "datetime-tz" | "duration" => decode(&as_str[1..as_str.len() - 1]),
        _ => decode(&as_str),
    }
}
