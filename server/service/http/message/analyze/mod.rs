/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ::concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use annotations::{encode_analyzed_fetch, encode_analyzed_function, FetchStructureAnnotationsResponse};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use query::analyse::AnalysedQuery;
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;
use structure::{encode_analyzed_pipeline, AnalyzedFunctionResponse, AnalyzedPipelineResponse};

use crate::service::http::message::body::JsonBody;

pub mod annotations;
pub mod structure;
pub(crate) mod studio;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyzeOptionsPayload {
    pub include_plan: Option<bool>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionAnalyzePayload {
    pub options: Option<AnalyzeOptionsPayload>,
    pub query: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalysedQueryResponse {
    pub source: String,
    pub query: AnalyzedPipelineResponse,
    pub preamble: Vec<AnalyzedFunctionResponse>,
    pub fetch: Option<FetchStructureAnnotationsResponse>,
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
    let query = encode_analyzed_pipeline(snapshot, type_manager, &structure.query, &annotations.query)?;
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

    use super::structure::AnalyzedPipelineResponse;

    pub(crate) struct FunctorContext<'a> {
        pub(super) pipeline: &'a AnalyzedPipelineResponse,
    }

    pub(crate) trait FunctorEncoded {
        fn encode_as_functor<'a>(&self, context: &FunctorContext<'a>) -> String;
    }

    pub mod functor_macros {
        macro_rules! encode_args {
            ($context:expr, { $( $arg:expr, )* } )   => {
                {
                    let arr: Vec<&dyn FunctorEncoded> = vec![ $($arg,)* ];
                    arr.into_iter().map(|s| s.encode_as_functor($context)).join(", ")
                }
            }
        }
        macro_rules! encode_functor_impl {
            ($context:expr, $func:ident $args:tt) => {
                std::format!("{}({})", std::stringify!($func), functor_macros::encode_args!($context, $args))
            };
            ($context:expr, ( $( $arg:ident, )* ) ) => {
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
