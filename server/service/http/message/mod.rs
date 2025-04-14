/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod authentication;
pub(crate) mod body;
pub mod database;
pub mod error;
pub mod query;
pub mod transaction;
pub mod user;
pub(crate) mod version;

macro_rules! stringify_kebab_case {
    ($t:tt) => {
        stringify!($t).replace("_", "-")
    };
}
pub(crate) use stringify_kebab_case;

macro_rules! from_request_parts_impl {
    ($struct_name:ident { $($field_name:ident : $field_ty:ty),* $(,)? }) => {
        #[axum::async_trait]
        impl<S> axum::extract::FromRequestParts<S> for $struct_name
        where
            S: Send + Sync,
        {
            type Rejection = axum::response::Response;

            async fn from_request_parts(
                parts: &mut axum::http::request::Parts,
                state: &S,
            ) -> Result<Self, Self::Rejection> {
                use axum::extract::Path;
                use axum::response::IntoResponse;
                use std::collections::HashMap;
                use std::str::FromStr;
                use http::StatusCode;
                use $crate::service::http::message::stringify_kebab_case;
                use crate::service::http::error::HttpServiceError;

                let params: Path<HashMap<String, String>> = Path::<HashMap<String, String>>::from_request_parts(parts, state)
                    .await
                    .map_err(IntoResponse::into_response)?;

                $(
                    let field_name = stringify_kebab_case!($field_name);
                    let $field_name = params.get(&field_name)
                        .ok_or_else(|| HttpServiceError::MissingPathParameter { parameter: field_name.clone() }.into_response())?
                        .parse::<$field_ty>()
                        .map_err(|_| HttpServiceError::InvalidPathParameter { parameter: field_name }.into_response())?;
                )*

                Ok(Self { $($field_name),* })
            }
        }
    };
}
pub(crate) use from_request_parts_impl;
