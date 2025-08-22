/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use error::{typedb_error, TypeDBError};

use crate::{
    authentication::AuthenticationError,
    error::{ArcServerStateError, LocalServerStateError},
    service::transaction_service::TransactionServiceError,
};

typedb_error!(
    pub HttpServiceError(component = "HTTP Service", prefix = "HSR") {
        Internal(1, "Internal error: {details}", details: String),
        JsonBodyExpected(2, "Cannot parse expected JSON body: {details}", details: String),
        RequestTimeout(3, "Request timeout."),
        NotFound(4, "Requested resource not found."),
        UnknownVersion(5, "Unknown API version '{version}'.", version: String),
        MissingPathParameter(6, "Requested resource not found: missing path parameter {parameter}.", parameter: String),
        InvalidPathParameter(7, "Requested resource not found: invalid path parameter {parameter}.", parameter: String),
        State(8, "State error.", typedb_source: ArcServerStateError),
        Authentication(9, "Authentication error.", typedb_source: AuthenticationError),
        Transaction(16, "Transaction error.", typedb_source: TransactionServiceError),
        QueryClose(17, "Error while closing single-query transaction.", typedb_source: TransactionServiceError),
        QueryCommit(18, "Error while committing single-query transaction.", typedb_source: TransactionServiceError),
    }
);

impl HttpServiceError {
    pub(crate) fn source(&self) -> &(dyn TypeDBError + Sync + '_) {
        self.source_typedb_error().unwrap_or(self)
    }

    pub(crate) fn format_source_trace(&self) -> String {
        self.stack_trace().join("\n").to_string()
    }

    pub(crate) fn operation_not_permitted() -> Self {
        Self::State { typedb_source: Arc::new(LocalServerStateError::OperationNotPermitted {}) }
    }

    pub(crate) fn no_open_transaction() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::NoOpenTransaction {} }
    }

    pub(crate) fn transaction_timeout() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::TransactionTimeout {} }
    }
}
