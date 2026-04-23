/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap};

use error::{typedb_error, TypeDBError};
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
    FailedQueryResponse {},
}

impl IntoGrpcStatus for ProtocolError {
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
            Self::FailedQueryResponse {} => Status::internal("Failed to send response"),
        }
    }
}

pub trait IntoProtocolErrorMessage {
    fn into_proto_error_message(self) -> typedb_protocol::Error;
}

impl<T: TypeDBError + Sync> IntoProtocolErrorMessage for T {
    fn into_proto_error_message(self) -> typedb_protocol::Error {
        let root_source = self.root_source_typedb_error();
        typedb_protocol::Error {
            error_code: root_source.code().to_string(),
            domain: root_source.component().to_string(),
            stack_trace: self.stack_trace(),
        }
    }
}

pub trait IntoGrpcStatus {
    fn into_status(self) -> Status;
}

impl IntoGrpcStatus for typedb_protocol::Error {
    fn into_status(self) -> Status {
        let mut details = ErrorDetails::with_error_info(self.error_code, self.domain, HashMap::new());
        details.set_debug_info(self.stack_trace, "");
        Status::with_error_details(Code::InvalidArgument, "Request generated error", details)
    }
}

impl<T: crate::error::ServerStateError + Sync> IntoGrpcStatus for T {
    fn into_status(self) -> Status {
        let category = self.error_response_category();
        let proto_error = self.into_proto_error_message();
        server_state_error_to_status(category, proto_error)
    }
}

impl IntoGrpcStatus for crate::error::ArcServerStateError {
    fn into_status(self) -> Status {
        let category = self.error_response_category();
        let proto_error = self.into_proto_error_message();
        server_state_error_to_status(category, proto_error)
    }
}

fn server_state_error_to_status(
    category: crate::error::ErrorResponseCategory,
    proto_error: typedb_protocol::Error,
) -> Status {
    use crate::error::ErrorResponseCategory;
    let (code, message, extra_metadata) = match category {
        ErrorResponseCategory::Redirect { grpc_address, .. }
        | ErrorResponseCategory::AuthenticatedRedirect { grpc_address, .. } => match grpc_address {
            Some(address) => {
                let mut metadata = HashMap::new();
                metadata.insert("address".to_string(), address);
                (Code::Unavailable, "Redirected", Some(("REDIRECT", metadata)))
            }
            None => (Code::Unavailable, "Unavailable", None),
        },
        ErrorResponseCategory::NotFound => (Code::NotFound, "Not found", None),
        ErrorResponseCategory::Unauthenticated => (Code::Unauthenticated, "Unauthenticated", None),
        ErrorResponseCategory::Forbidden => (Code::PermissionDenied, "Forbidden", None),
        ErrorResponseCategory::NotImplemented => (Code::Unimplemented, "Not implemented", None),
        ErrorResponseCategory::Unavailable => (Code::Unavailable, "Unavailable", None),
        ErrorResponseCategory::InvalidRequest => return proto_error.into_status(),
        ErrorResponseCategory::Internal => (Code::Internal, "Internal error", None),
    };

    let mut details = ErrorDetails::with_error_info(proto_error.error_code, proto_error.domain, HashMap::new());
    details.set_debug_info(proto_error.stack_trace, "");
    if let Some((reason, metadata)) = extra_metadata {
        details.set_error_info(reason, "typedb", metadata);
    }
    Status::with_error_details(code, message, details)
}

typedb_error! {
    pub(crate) GrpcServiceError(component = "GRPC Service", prefix = "GSR") {
        UnexpectedMissingField(1, "Invalid request: missing field '{field}'.", field: String),
    }
}
