/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};
use error::TypeDBError;


pub(crate) enum ProtocolError {
    MissingField { name: &'static str, description: &'static str },
    TransactionAlreadyOpen {},
    TransactionClosed {},
}

impl Into<Status> for ProtocolError {
    fn into(self) -> Status {
        match self {
            Self::MissingField { name, description } => {
                Status::with_error_details(
                    Code::InvalidArgument,
                    "Bad request",
                    ErrorDetails::with_bad_request_violation(
                        name,
                        format!("{}. Check client-server compatibility?", description),
                    ),
                )
            }
            Self::TransactionAlreadyOpen {} => {
                Status::already_exists("Transaction already open.")
            }
            Self::TransactionClosed {} => {
                Status::new(Code::InvalidArgument, "Transaction already closed, no further operations possible.")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum TransactionServiceError {
    UnrecognisedTransactionType { enum_variant: i32 },
    DatabaseNotFound { name: String },
    CannotCommitReadTransaction {},
}

impl fmt::Display for TransactionServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Error for TransactionServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnrecognisedTransactionType { .. }
            | Self::CannotCommitReadTransaction { .. }
            | Self::DatabaseNotFound { .. } => None,
        }
    }
}

trait StatusConvertible {
    fn into_status(self) -> Status;
}

impl<T: TypeDBError> StatusConvertible for T {
    fn into_status(self) -> Status {
        let details = ErrorDetails::new();

        let root_source = self.root_source_typedb_error();
        root_source.();

    }
}

impl<T: StatusConvertible> Into<Status> for T {

}
