/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use error::TypeDBError;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};
use uuid::Uuid;

pub(crate) enum ProtocolError {
    MissingField { name: &'static str, description: &'static str },
    TransactionAlreadyOpen {},
    TransactionClosed {},
    UnrecognisedTransactionType { enum_variant: i32 },
    QueryStreamNotFound { query_request_id: Uuid },
}

impl StatusConvertible for ProtocolError {
    fn into_status(self) -> Status {
        match self {
            Self::MissingField { name, description } => Status::with_error_details(
                Code::InvalidArgument,
                "Bad request",
                ErrorDetails::with_bad_request_violation(
                    name,
                    format!("{}. Check client-server compatibility?", description),
                ),
            ),
            Self::TransactionAlreadyOpen {} => Status::already_exists("Transaction already open."),
            Self::TransactionClosed {} => {
                Status::new(Code::InvalidArgument, "Transaction already closed, no further operations possible.")
            }
            ProtocolError::UnrecognisedTransactionType { enum_variant, .. } => Status::with_error_details(
                Code::InvalidArgument,
                "Bad request",
                ErrorDetails::with_bad_request_violation(
                    "transaction_type",
                    format!(
                        "Unrecognised transaction type variant: {enum_variant}. Check client-server compatibility?"
                    ),
                ),
            ),
            ProtocolError::QueryStreamNotFound { query_request_id } => Status::new(
                Code::NotFound,
                format!(
                    "Query stream with protocol id '{:?}' was not found in the transaction. \
                    The transaction could be closed, committed, rolled back, or this is an error state.",
                    query_request_id
                ),
            ),
        }
    }
}

pub(crate) trait StatusConvertible {
    fn into_status(self) -> Status;
}

impl<T: TypeDBError> StatusConvertible for T {
    fn into_status(self) -> Status {
        let root_source = self.root_source_typedb_error();
        let code = root_source.code();
        let domain = root_source.domain();
        let mut metadata = HashMap::new();
        metadata.insert(String::from("description"), root_source.format_description());
        let mut details = ErrorDetails::with_error_info(code, domain, metadata);
        let mut stack_trace = Vec::with_capacity(4); // definitely non-zero!

        let mut error: &dyn TypeDBError = &self;
        stack_trace.push(error.format_description());
        while let Some(source) = error.source_typedb_error() {
            error = source;
            stack_trace.push(error.format_description());
        }
        details.set_debug_info(stack_trace, "");

        Status::with_error_details(Code::InvalidArgument, "Request generated error", details)
    }
}
