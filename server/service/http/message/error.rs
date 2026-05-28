/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::response::{IntoResponse, Response};
use error::TypeDBError;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    error::ErrorResponseCategory,
    service::{
        http::{error::HttpServiceError, message::body::JsonBody},
        transaction_service::TransactionServiceError,
    },
};

enum RedirectOutcome {
    Misdirected(String),
    Unavailable,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

/// Response for authenticated endpoints that cannot be served by this server.
/// Unlike a redirect, the client must re-authenticate against the primary.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MisdirectedResponse {
    #[serde(flatten)]
    pub error: ErrorResponse,
    pub primary_address: String,
}

impl IntoResponse for HttpServiceError {
    fn into_response(self) -> Response {
        if let Some(outcome) = find_redirect_outcome(&self) {
            return match outcome {
                RedirectOutcome::Misdirected(primary_address) => (
                    StatusCode::MISDIRECTED_REQUEST,
                    JsonBody(MisdirectedResponse { error: encode_error(self), primary_address }),
                )
                    .into_response(),
                RedirectOutcome::Unavailable => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            };
        }

        let code = match &self {
            HttpServiceError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            HttpServiceError::JsonBodyExpected { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::RequestTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
            HttpServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
            HttpServiceError::UnknownVersion { .. } => StatusCode::NOT_FOUND,
            HttpServiceError::MissingPathParameter { .. } => StatusCode::NOT_FOUND,
            HttpServiceError::InvalidPathParameter { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::State { typedb_source } => match typedb_source.error_response_category() {
                ErrorResponseCategory::NotFound => StatusCode::NOT_FOUND,
                ErrorResponseCategory::Unauthenticated => StatusCode::UNAUTHORIZED,
                ErrorResponseCategory::Forbidden => StatusCode::FORBIDDEN,
                ErrorResponseCategory::NotImplemented => StatusCode::NOT_IMPLEMENTED,
                ErrorResponseCategory::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                // Redirect is handled by `find_redirect_state` above.
                ErrorResponseCategory::Redirect { .. } => StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponseCategory::InvalidRequest => StatusCode::BAD_REQUEST,
                ErrorResponseCategory::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            },
            HttpServiceError::Authentication { .. } => StatusCode::UNAUTHORIZED,
            HttpServiceError::Transaction { typedb_source } => match typedb_source {
                TransactionServiceError::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                TransactionServiceError::CannotCommitReadTransaction { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::CannotRollbackReadTransaction { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::TransactionFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::DataCommitFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::SchemaCommitFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::QueryParseFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::SchemaQueryRequiresSchemaTransaction { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::WriteQueryRequiresSchemaOrWriteTransaction { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::SchemaQueryFailedAbortingTransaction { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::QueryFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::AnalyseQueryFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::AnalyseQueryExpectsPipeline { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::NoOpenTransaction { .. } => StatusCode::NOT_FOUND,
                TransactionServiceError::QueryInterrupted { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::QueryStreamNotFound { .. } => StatusCode::NOT_FOUND,
                TransactionServiceError::QueueCleanupFailed { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::PipelineExecution { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::TransactionTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
                TransactionServiceError::InvalidPrefetchSize { .. } => StatusCode::BAD_REQUEST,
                TransactionServiceError::CannotOpen { .. } => StatusCode::BAD_REQUEST,
            },
            HttpServiceError::QueryClose { .. } => StatusCode::BAD_REQUEST,
            HttpServiceError::QueryCommit { .. } => StatusCode::BAD_REQUEST,
        };
        (code, JsonBody(encode_error(self))).into_response()
    }
}

fn find_redirect_outcome(error: &HttpServiceError) -> Option<RedirectOutcome> {
    match error.to_service_error()?.error_response_category() {
        ErrorResponseCategory::Redirect { http_address: Some(addr), .. } => Some(RedirectOutcome::Misdirected(addr)),
        ErrorResponseCategory::Redirect { .. } => Some(RedirectOutcome::Unavailable),
        _ => None,
    }
}

pub(crate) fn encode_error(error: HttpServiceError) -> ErrorResponse {
    ErrorResponse { code: error.root_source_typedb_error().code().to_string(), message: error.format_source_trace() }
}
