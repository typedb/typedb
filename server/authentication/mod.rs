/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use axum::RequestPartsExt;
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use error::typedb_error;
use http::Extensions;
use resource::constants::server::DEFAULT_USER_NAME;
use tonic::metadata::MetadataMap;
use user::permission_manager::UserSession;

use crate::state::ServerState;

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
    server_state: Arc<ServerState>,
    request: http::Request<T>,
) -> Result<http::Request<T>, AuthenticationError> {
    let (mut parts, body) = request.into_parts();

    match extract_parts_authorization_token(parts.clone()).await {
        Some(token) => {
            let session =
                server_state.users().token_get_session(&token).await.ok_or(AuthenticationError::InvalidToken {})?;
            parts.extensions.insert(Accessor::Session(session));
            Ok(http::Request::from_parts(parts, body))
        }
        None => Err(AuthenticationError::MissingToken {}),
    }
}

#[derive(Debug, Clone)]
pub enum Accessor {
    Default,
    Session(Arc<UserSession>),
}

impl Accessor {
    // TODO: must remove and replace calls with the actual accessor.
    pub(crate) fn default_to_do() -> Self {
        Self::Default
    }
}

impl Accessor {
    pub fn from_extensions(extensions: &Extensions) -> Result<Self, AuthenticationError> {
        extensions.get::<Self>().cloned().ok_or_else(|| AuthenticationError::CorruptedAccessor {})
    }

    pub fn username(&self) -> &str {
        match self {
            Accessor::Default => DEFAULT_USER_NAME,
            Accessor::Session(session) => session.username(),
        }
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
