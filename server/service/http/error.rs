/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::{database::DatabaseCreateError, DatabaseDeleteError};
use error::{typedb_error, TypeDBError};
use user::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

use crate::{
    authentication::AuthenticationError,
    service::{transaction_service::TransactionServiceError, ServiceError},
};

typedb_error!(
    pub HttpServiceError(component = "HTTP Service", prefix = "HSR") {
        Internal(1, "Internal error: {details}", details: String),
        JsonBodyExpected(2, "Cannot parse expected JSON body: {details}", details: String),
        RequestTimeout(3, "Request timeout."),
        NotFound(4, "Requested resource not found."),
        MissingPathParameter(5, "Requested resource not found: missing path parameter {parameter}.", parameter: String),
        InvalidPathParameter(6, "Requested resource not found: invalid path parameter {parameter}.", parameter: String),
        Service(7, "Service error.", typedb_source: ServiceError),
        Authentication(8, "Authentication error.", typedb_source: AuthenticationError),
        DatabaseCreate(9, "Database create error.", typedb_source: DatabaseCreateError),
        DatabaseDelete(10, "Database delete error.", typedb_source: DatabaseDeleteError),
        UserCreate(11, "User create error.", typedb_source: UserCreateError),
        UserUpdate(12, "User update error.", typedb_source: UserUpdateError),
        UserDelete(13, "User delete error.", typedb_source: UserDeleteError),
        UserGet(14, "User get error.", typedb_source: UserGetError),
        Transaction(15, "Transaction error.", typedb_source: TransactionServiceError),
        QueryClose(16, "Error while closing single-query transaction.", typedb_source: TransactionServiceError),
        QueryCommit(17, "Error while committing single-query transaction.", typedb_source: TransactionServiceError),
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
        Self::Service { typedb_source: ServiceError::OperationNotPermitted {} }
    }

    pub(crate) fn no_open_transaction() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::NoOpenTransaction {} }
    }
}
