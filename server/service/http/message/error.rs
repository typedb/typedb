/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::response::{IntoResponse, Response};
use error::TypeDBError;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::service::{
    http::{error::HttpServiceError, message::body::JsonBody},
    ServiceError,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

impl IntoResponse for HttpServiceError {
    fn into_response(self) -> Response {
        let code = match &self {
            HttpServiceError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            HttpServiceError::JsonBodyExpected { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::RequestTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
            HttpServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
            HttpServiceError::MissingPathParameter { .. } => StatusCode::NOT_FOUND,
            HttpServiceError::InvalidPathParameter { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::Service { typedb_source } => match typedb_source {
                ServiceError::Unimplemented { .. } => StatusCode::NOT_IMPLEMENTED,
                ServiceError::OperationNotPermitted { .. } => StatusCode::FORBIDDEN,
                ServiceError::DatabaseDoesNotExist { .. } => StatusCode::NOT_FOUND,
                ServiceError::UserDoesNotExist { .. } => StatusCode::NOT_FOUND,
            },
            HttpServiceError::Authentication { .. } => StatusCode::UNAUTHORIZED,
            HttpServiceError::DatabaseCreate { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::DatabaseDelete { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::UserCreate { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::UserUpdate { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::UserDelete { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::UserGet { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::Transaction { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::QueryClose { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::QueryCommit { .. } => StatusCode::BAD_REQUEST,
        };
        (code, JsonBody(encode_error(self))).into_response()
    }
}

pub(crate) fn encode_error(error: HttpServiceError) -> ErrorResponse {
    ErrorResponse { code: error.root_source_typedb_error().code().to_string(), message: error.format_source_trace() }
}
