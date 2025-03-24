/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::{
    async_trait,
    extract::FromRequestParts,
    response::{IntoResponse, Response},
};
use http::request::Parts;
use serde::{Deserialize, Serialize};

use crate::{authentication::Accessor, service::http::error::HttpServiceError};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SigninPayload {
    pub username: String,
    pub password: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for Accessor
where
    S: Send + Sync,
{
    type Rejection = HttpServiceError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Accessor::from_extensions(&parts.extensions)
            .map_err(|typedb_source| HttpServiceError::Authentication { typedb_source })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenResponse {
    pub token: String,
}

pub(crate) fn encode_token(token: String) -> TokenResponse {
    TokenResponse { token }
}
