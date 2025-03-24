/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr};

use axum::{
    async_trait,
    extract::{FromRequest, FromRequestParts, Path},
    response::{IntoResponse, Response},
    RequestExt, RequestPartsExt,
};
use futures::TryFutureExt;
use http::{request::Parts, StatusCode};

use crate::service::http::error::HttpServiceError;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd)]
pub(crate) enum ProtocolVersion {
    V1,
}

impl ProtocolVersion {
    const VERSION_PARAM: &'static str = "version";
}

#[async_trait]
impl<S> FromRequestParts<S> for ProtocolVersion
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let params: Path<HashMap<String, String>> = parts.extract().await.map_err(IntoResponse::into_response)?;
        let version = params.get(Self::VERSION_PARAM).ok_or_else(|| {
            HttpServiceError::MissingPathParameter { parameter: Self::VERSION_PARAM.to_string() }.into_response()
        })?;
        ProtocolVersion::from_str(version.as_str()).map_err(|error| error.into_response())
    }
}

#[derive(Debug)]
pub(crate) struct ProtocolVersionParseError {
    pub(crate) version: String,
}

impl IntoResponse for ProtocolVersionParseError {
    fn into_response(self) -> Response {
        let error = format!("Unknown version '{}'", self.version);
        (StatusCode::NOT_FOUND, error).into_response()
    }
}

impl FromStr for ProtocolVersion {
    type Err = ProtocolVersionParseError;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        match version {
            "v1" => Ok(ProtocolVersion::V1),
            _ => Err(ProtocolVersionParseError { version: version.to_string() }),
        }
    }
}
