/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::{database::DatabaseCreateError, DatabaseDeleteError};
use error::{typedb_error, TypeDBError};

use crate::{
    authentication::AuthenticationError, service::transaction_service::TransactionServiceError, state::StateError,
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
        Service(8, "Service error.", typedb_source: StateError),
        Authentication(9, "Authentication error.", typedb_source: AuthenticationError),
        DatabaseCreate(10, "Database create error.", typedb_source: DatabaseCreateError),
        DatabaseDelete(11, "Database delete error.", typedb_source: DatabaseDeleteError),
        DatabaseSchema(19, "Database schema error.", typedb_source: StateError),
        DatabaseTypeSchema(20, "Database type schema error.", typedb_source: StateError),
        UserCreate(12, "User create error.", typedb_source: StateError),
        UserUpdate(13, "User update error.", typedb_source: StateError),
        UserDelete(14, "User delete error.", typedb_source: StateError),
        UserGet(15, "User get error.", typedb_source: StateError),
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
        Self::Service { typedb_source: StateError::OperationNotPermitted {} }
    }

    pub(crate) fn no_open_transaction() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::NoOpenTransaction {} }
    }

    pub(crate) fn transaction_timeout() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::TransactionTimeout {} }
    }
}
