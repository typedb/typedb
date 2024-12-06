/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap};

use error::TypeDBError;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

// Errors caused by incorrect implementation or usage of the network protocol.
// Note: NOT a typedb_error!(), since we want go directly to Status
#[derive(Debug)]
pub(crate) enum ProtocolError {
    MissingField {
        name: &'static str,
        description: &'static str,
    },
    TransactionAlreadyOpen {},
    TransactionClosed {},
    UnrecognisedTransactionType {
        enum_variant: i32,
    },
    IncompatibleProtocolVersion {
        server_protocol_version: i32,
        driver_protocol_version: i32,
        driver_lang: String,
        driver_version: String,
    },
    ErrorCompletingWrite {},
    FailedQueryResponse {},
}

impl IntoGRPCStatus for ProtocolError {
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
            Self::IncompatibleProtocolVersion {
                server_protocol_version,
                driver_protocol_version,
                driver_version,
                driver_lang,
            } => {
                let required_driver_age = match server_protocol_version.cmp(&driver_protocol_version) {
                    Ordering::Less => "an older",
                    Ordering::Equal => unreachable!("Incompatible protocol version should only be thrown "),
                    Ordering::Greater => "a newer",
                };

                Status::failed_precondition(format!(
                    r#"
                    Incompatible driver version. This '{driver_lang}' driver version '{driver_version}' implements protocol version {driver_protocol_version},
                    while the server supports network protocol version {server_protocol_version}. Please use {required_driver_age} driver that is compatible with this server.
                    "#
                ))
            }
            Self::UnrecognisedTransactionType { enum_variant, .. } => Status::with_error_details(
                Code::InvalidArgument,
                "Bad request",
                ErrorDetails::with_bad_request_violation(
                    "transaction_type",
                    format!(
                        "Unrecognised transaction type variant: {enum_variant}. Check client-server compatibility?"
                    ),
                ),
            ),
            Self::ErrorCompletingWrite {} => {
                Status::new(Code::Internal, "Error completing currently executing write query.")
            }
            Self::FailedQueryResponse {} => Status::internal("Failed to send response"),
        }
    }
}

pub(crate) trait IntoProtocolErrorMessage {
    fn into_error_message(self) -> typedb_protocol::Error;
}

impl<T: TypeDBError + Sync> IntoProtocolErrorMessage for T {
    fn into_error_message(self) -> typedb_protocol::Error {
        let root_source = self.root_source_typedb_error();
        let code = root_source.code();
        let component = root_source.component();

        let mut stack_trace = Vec::with_capacity(4); // definitely non-zero!
        let mut error: &dyn TypeDBError = &self;
        stack_trace.push(error.format_code_and_description());
        while let Some(source) = error.source_typedb_error() {
            error = source;
            stack_trace.push(error.format_code_and_description());
        }
        stack_trace.reverse();
        typedb_protocol::Error { error_code: code.to_string(), domain: component.to_string(), stack_trace }
    }
}

pub(crate) trait IntoGRPCStatus {
    fn into_status(self) -> Status;
}

impl IntoGRPCStatus for typedb_protocol::Error {
    fn into_status(self) -> Status {
        let mut details = ErrorDetails::with_error_info(self.error_code, self.domain, HashMap::new());
        details.set_debug_info(self.stack_trace, "");
        Status::with_error_details(Code::InvalidArgument, "Request generated error", details)
    }
}
