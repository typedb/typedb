/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ::concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use options::QueryOptions;
use query::analyse::AnalysedQuery;
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_PREFETCH_SIZE,
};
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;
use tracing::Value;

use crate::service::{
    http::{
        message::{
            body::JsonBody,
            query::{
                annotations::{encode_analyzed_fetch, encode_analyzed_function, FetchStructureAnnotationsResponse},
                query_structure::{
                    encode_analyzed_pipeline, AnalyzedFunctionResponse, AnalyzedPipelineResponse,
                    PipelineStructureResponseForStudio,
                },
            },
            transaction::TransactionOpenPayload,
        },
        transaction_service::QueryAnswer,
    },
    AnswerType, QueryType,
};

pub mod annotations;
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
    pub query: Option<PipelineStructureResponseForStudio>,
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
        query: pipeline_structure.map(|structure| structure.into()),
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
pub struct AnalysedQueryResponse {
    pub source: String,
    pub(super) query: AnalyzedPipelineResponse,
    pub(super) preamble: Vec<AnalyzedFunctionResponse>,
    pub(super) fetch: Option<FetchStructureAnnotationsResponse>,
}

impl IntoResponse for AnalysedQueryResponse {
    fn into_response(self) -> Response {
        let code = StatusCode::OK;
        let body = JsonBody(self);
        (code, body).into_response()
    }
}

pub fn encode_analyzed_query(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    analysed_query: AnalysedQuery,
) -> Result<AnalysedQueryResponse, Box<ConceptReadError>> {
    let AnalysedQuery { source, structure, annotations } = analysed_query;
    let preamble = structure
        .preamble
        .into_iter()
        .zip(annotations.preamble.into_iter())
        .map(|(structure, annotations)| encode_analyzed_function(snapshot, type_manager, structure, annotations))
        .collect::<Result<Vec<_>, _>>()?;
    let query = encode_analyzed_pipeline(snapshot, type_manager, &structure.query, &annotations.query, true)?;
    let fetch = annotations
        .fetch
        .map(|fetch| {
            encode_analyzed_fetch(snapshot, type_manager, fetch)
                .map(|fields| FetchStructureAnnotationsResponse::Object { possible_fields: fields })
        })
        .transpose()?;
    Ok(AnalysedQueryResponse { source, query, preamble, fetch })
}

#[cfg(debug_assertions)]
pub mod bdd {
    use std::collections::HashMap;

    use itertools::Itertools;

    use crate::service::http::message::query::query_structure::AnalyzedPipelineResponse;

    pub(crate) struct FunctorContext<'a> {
        pub(super) pipeline: &'a AnalyzedPipelineResponse,
    }

    pub(crate) trait FunctorEncoded {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String;
    }

    pub mod functor_macros {
        macro_rules! encode_args {
        ($context:ident, { $( $arg:ident, )* } )   => {
            {
                let arr: Vec<&dyn FunctorEncoded> = vec![ $($arg,)* ];
                arr.into_iter().map(|s| s.encode_as_functor($context)).join(", ")
            }
        }
    }
        macro_rules! encode_functor_impl {
        ($context:ident, $func:ident $args:tt) => {
            std::format!("{}({})", std::stringify!($func), functor_macros::encode_args!($context, $args))
        };
        ($context:ident, ( $( $arg:ident, )* ) ) => {
            functor_macros::encode_args!($context, { $( $arg, )* } )
        };
    }

        macro_rules! add_ignored_fields {
        ($qualified:path : { $( $arg:ident, )* }) => { $qualified { $( $arg, )* .. } };
        ($qualified:path : ($( $arg:ident, )*)) => { $qualified ( $( $arg, )* .. ) };
    }

        macro_rules! encode_functor {
        ($context:ident, $what:ident as struct $struct_name:ident  $fields:tt) => {
            functor_macros::encode_functor!($context, $what => [ $struct_name => $struct_name $fields, ])
        };
        ($context:ident, $what:ident as struct $struct_name:ident $fields:tt named $renamed:ident ) => {
            functor_macros::encode_functor!($context, $what => [ $struct_name => $renamed $fields, ])
        };
        ($context:ident, $what:ident as enum $enum_name:ident [ $($variant:ident $fields:tt |)* ]) => {
            functor_macros::encode_functor!($context, $what => [ $( $enum_name::$variant => $variant $fields ,)* ])
        };
        ($context:ident, $what:ident => [ $($qualified:path => $func:ident $fields:tt, )* ]) => {
            match $what {
                $( functor_macros::add_ignored_fields!($qualified : $fields) => {
                    functor_macros::encode_functor_impl!($context, $func $fields)
                })*
            }
        };
    }

        macro_rules! impl_functor_for_impl {
            ($which:ident => |$self:ident, $context:ident| $block:block) => {
                impl FunctorEncoded for $which {
                    fn encode_as_functor<'a>($self: &Self, $context: &FunctorContext<'a>) -> String {
                        $block
                    }
                }
            };
        }

        macro_rules! impl_functor_for {
        (struct $struct_name:ident $fields:tt) => {
            functor_macros::impl_functor_for!(struct $struct_name $fields named $struct_name);
        };
        (struct $struct_name:ident $fields:tt named $renamed:ident) => {
            functor_macros::impl_functor_for_impl!($struct_name => |self, context| {
                functor_macros::encode_functor!(context, self as struct $struct_name $fields named $renamed)
            });
        };
        (enum $enum_name:ident [ $($func:ident $fields:tt |)* ]) => {
            functor_macros::impl_functor_for_impl!($enum_name => |self, context| {
                functor_macros::encode_functor!(context, self as enum $enum_name [ $($func $fields |)* ])
            });
        };
        (primitive $primitive:ident) => {
            functor_macros::impl_functor_for_impl!($primitive => |self, _context| { self.to_string() });
        };
    }
        macro_rules! impl_functor_for_multi {
        (|$self:ident, $context:ident| [ $( $type_name:ident => $block:block )* ]) => {
            $ (functor_macros::impl_functor_for_impl!($type_name => |$self, $context| $block); )*
        };
    }

        pub(crate) use add_ignored_fields;
        pub(crate) use encode_args;
        pub(crate) use encode_functor;
        pub(crate) use encode_functor_impl;
        pub(crate) use impl_functor_for;
        pub(crate) use impl_functor_for_impl;
        pub(crate) use impl_functor_for_multi;
    }

    functor_macros::impl_functor_for!(primitive String);
    functor_macros::impl_functor_for!(primitive u64);
    impl<T: FunctorEncoded> FunctorEncoded for Vec<T> {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            std::format!("[{}]", self.iter().map(|v| v.encode_as_functor(context)).join(", "))
        }
    }

    impl<K: FunctorEncoded, V: FunctorEncoded> FunctorEncoded for HashMap<K, V> {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            std::format!(
                "{{ {} }}",
                self.iter()
                    .map(|(k, v)| {
                        std::format!("{}: {}", k.encode_as_functor(context), v.encode_as_functor(context))
                    })
                    .sorted_by(|a, b| a.cmp(b))
                    .join(", ")
            )
        }
    }

    impl<T: FunctorEncoded> FunctorEncoded for Option<T> {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String {
            self.as_ref().map(|inner| inner.encode_as_functor(context)).unwrap_or("<NONE>".to_owned())
        }
    }
}
