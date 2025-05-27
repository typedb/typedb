/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use axum::RequestPartsExt;
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use error::typedb_error;
use http::Extensions;
use tonic::metadata::MetadataMap;

use crate::state::ServerState;
use crate::state::LocalServerState;

pub(crate) mod credential_verifier;
pub(crate) mod token_manager;

pub const HTTP_AUTHORIZATION_FIELD: &str = "authorization";
pub const HTTP_BEARER_PREFIX: &str = "Bearer ";

pub(crate) async fn extract_parts_authorization_token(mut parts: http::request::Parts) -> Option<String> {
    parts
        .extract::<TypedHeader<Authorization<Bearer>>>()
        .await
        .map(|header| {
            let TypedHeader(Authorization(bearer)) = header;
            bearer.token().to_string()
        })
        .ok()
}

pub(crate) fn extract_metadata_authorization_token(metadata: &MetadataMap) -> Option<String> {
    let Some(Ok(authorization)) = metadata.get(HTTP_AUTHORIZATION_FIELD).map(|value| value.to_str()) else {
        return None;
    };
    authorization.strip_prefix(HTTP_BEARER_PREFIX).map(|token| token.to_string())
}

pub(crate) fn extract_metadata_accessor(metadata: &MetadataMap) -> Option<String> {
    let Some(Ok(authorization)) = metadata.get(HTTP_AUTHORIZATION_FIELD).map(|value| value.to_str()) else {
        return None;
    };
    authorization.strip_prefix(HTTP_BEARER_PREFIX).map(|token| token.to_string())
}

pub(crate) async fn authenticate<T>(
    server_state: Arc<Box<dyn ServerState + Send + Sync>>,
    request: http::Request<T>,
) -> Result<http::Request<T>, AuthenticationError> {
    let (mut parts, body) = request.into_parts();

    match extract_parts_authorization_token(parts.clone()).await {
        Some(token) => {
            let accessor = server_state.token_get_owner(&token).await.ok_or(AuthenticationError::InvalidToken {})?;
            parts.extensions.insert(Accessor(accessor));
            Ok(http::Request::from_parts(parts, body))
        }
        None => Err(AuthenticationError::MissingToken {}),
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Accessor(pub(crate) String);

impl Accessor {
    pub fn from_extensions(extensions: &Extensions) -> Result<Self, AuthenticationError> {
        extensions.get::<Self>().cloned().ok_or_else(|| AuthenticationError::CorruptedAccessor {})
    }
}

// CAREFUL: Do not reorder these errors as we depend on errors codes in drivers.
typedb_error! {
    pub AuthenticationError(component = "Authentication", prefix = "AUT") {
        InvalidCredential(1, "Invalid credential supplied."),
        MissingToken(2, "Missing token (expected as the authorization bearer)."),
        InvalidToken(3, "Invalid token supplied."),
        CorruptedAccessor(4, "Could not identify the mandatory request's accessor. This might be an authentication bug."),
    }
}
