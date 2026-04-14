/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, io, net::SocketAddr, sync::Arc};

use concept::error::ConceptReadError;
use database::{
    database::DatabaseCreateError,
    transaction::{DataCommitError, SchemaCommitError, TransactionError},
    DatabaseDeleteError, DatabaseOpenError,
};
use error::{typedb_error, TypeDBError};
use ir::pipeline::FunctionReadError;
use tokio_rustls::rustls::{
    pki_types::pem::Error as RustlsCertError, server::VerifierBuilderError as RustlsVerifierError,
};
use user::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

use crate::{
    authentication::{token_manager::TokenManagerError, AuthenticationError},
    service::{export_service::DatabaseExportError, import_service::DatabaseImportServiceError},
};

pub enum ErrorResponseCategory {
    NotFound,
    Unauthenticated,
    Forbidden,
    NotImplemented,
    Unavailable,
    Redirect { grpc_address: Option<String>, http_address: Option<String> },
    InvalidRequest,
    Internal,
}

pub trait ServerStateError: TypeDBError + Send + Sync + Debug + 'static {
    fn error_response_category(&self) -> ErrorResponseCategory;
}

pub type ArcServerStateError = Arc<dyn ServerStateError>;

typedb_error! {
    pub ServerOpenError(component = "Server open", prefix = "SRO") {
        NotADirectory(1, "Invalid path '{path}': not a directory.", path: String),
        CouldNotReadServerIDFile(2, "Could not read data from server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateServerIDFile(3, "Could not write data to server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateDataDirectory(4, "Could not create data directory in '{path}'.", path: String, source: Arc<io::Error>),
        InvalidServerID(5, "Server ID read from '{path}' is invalid. Delete the corrupted file and try again.", path: String),
        DatabaseOpen(6, "Could not open database.", typedb_source: DatabaseOpenError),
        TokenConfiguration(7, "Token configuration error.", typedb_source: TokenManagerError),
        MissingTLSCertificate(8, "TLS certificate path must be specified when encryption is enabled."),
        MissingTLSCertificateKey(9, "TLS certificate key path must be specified when encryption is enabled."),
        HttpConflictingAddress(10, "Configuring HTTP and gRPC on the same address {address} is not supported.", address: SocketAddr),
        AdminConflictingAddress(11, "Configuring admin and public service on the same address {address} is not supported.", address: SocketAddr),
        GrpcServe(12, "Could not serve gRPC on {address}.", address: SocketAddr, source: Arc<tonic::transport::Error>),
        GrpcCouldNotReadTlsCertificate(13, "Could not read TLS certificate from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcCouldNotReadTlsCertificateKey(14, "Could not read TLS certificate key from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcCouldNotReadRootCa(15, "Could not read root CA from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcInvalidRootCa(16, "Invalid root CA for the gRPC server.", source: Arc<io::Error>),
        GrpcTlsFailedConfiguration(17, "Failed to configure TLS for the gRPC server.", source: Arc<tonic::transport::Error>),
        HttpServe(18, "Could not serve HTTP on {address}.", address: SocketAddr, source: Arc<io::Error>),
        HttpCouldNotReadTlsCertificate(19, "Could not read TLS certificate from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpCouldNotReadTlsCertificateKey(20, "Could not read TLS certificate key from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpCouldNotReadRootCa(21, "Could not read root CA from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpInvalidRootCa(22, "Invalid root CA for the HTTP server.", source: Arc<RustlsVerifierError>),
        HttpTlsFailedConfiguration(23, "Failed to configure TLS for the HTTP server.", source: Arc<tokio_rustls::rustls::Error>),
        HttpTlsUnsetDefaultCryptoProvider(24, "Failed to install default crypto provider for the HTTP server TLS configuration."),
        HttpTlsPemFileError(25, "Invalid PEM file specified for the HTTP server.", source: Arc<tokio_rustls::rustls::pki_types::pem::Error>),
        ServerState(26, "Invalid server state.", typedb_source: ArcServerStateError),
        AddressResolutionFailed(27, "Could not resolve address '{address}'.", address: String, source: Arc<io::Error>),
        AddressResolutionEmpty(28, "Could not resolve address '{address}' to any IP address.", address: String),
        AdminServe(29, "Could not serve admin on {address}.", address: SocketAddr, source: Arc<tonic::transport::Error>),
    }
}

typedb_error! {
    pub LocalServerStateError(component = "Server state", prefix = "SRV") {
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationNotPermitted(2, "The user is not permitted to execute the operation."),
        DatabaseNotFound(3, "Database '{name}' not found.", name: String),
        UserNotFound(4, "User not found."),
        FailedToOpenPrerequisiteTransaction(5, "Failed to open transaction, which is a prerequisite for the operation.", typedb_source: TransactionError),
        ConceptReadError(6, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        FunctionReadError(7, "Error reading functions.", typedb_source: FunctionReadError),
        UserCannotBeRetrieved(8, "Unable to retrieve user.", typedb_source: UserGetError),
        UserCannotBeCreated(9, "Unable to create user.", typedb_source: UserCreateError),
        UserCannotBeUpdated(10, "Unable to update user.", typedb_source: UserUpdateError),
        UserCannotBeDeleted(11, "Unable to delete user.", typedb_source: UserDeleteError),
        DatabaseCannotBeCreated(12, "Unable to create database.", typedb_source: DatabaseCreateError),
        DatabaseCannotBeDeleted(13, "Unable to delete database.", typedb_source: DatabaseDeleteError),
        NotInitialised(14, "Not yet initialised."),
        AuthenticationError(15, "Error when authenticating.", typedb_source: AuthenticationError),
        DatabaseExport(16, "Database export error", typedb_source: DatabaseExportError),
        DatabaseImport(17, "Database import error.", typedb_source: DatabaseImportServiceError),
        DatabaseSchemaCommitFailed(18, "Schema commit failed.", typedb_source: SchemaCommitError),
        DatabaseDataCommitFailed(19, "Data commit failed.", typedb_source: DataCommitError),
        DatabaseCommitRecordExistsFailed(20, "Commit record check failed.", typedb_source: DatabaseOpenError),
        NotSupportedByDistribution(21, "Not supported by this distribution: {description}", description: String),
        TransactionOpenFailed(22, "Failed to open transaction.", typedb_source: TransactionError),
    }
}

impl ServerStateError for LocalServerStateError {
    fn error_response_category(&self) -> ErrorResponseCategory {
        use ErrorResponseCategory::*;

        use crate::authentication::AuthenticationError;
        match self {
            Self::Unimplemented { .. } | Self::NotSupportedByDistribution { .. } => NotImplemented,

            Self::AuthenticationError { typedb_source } => match typedb_source {
                AuthenticationError::CorruptedAccessor { .. } => Internal,
                _ => Unauthenticated,
            },

            Self::OperationNotPermitted { .. } => Forbidden,

            Self::DatabaseNotFound { .. } | Self::UserNotFound { .. } => NotFound,

            Self::ConceptReadError { .. } | Self::FunctionReadError { .. } => Internal,

            Self::NotInitialised { .. }
            | Self::DatabaseSchemaCommitFailed { .. }
            | Self::DatabaseDataCommitFailed { .. }
            | Self::DatabaseCommitRecordExistsFailed { .. }
            | Self::FailedToOpenPrerequisiteTransaction { .. }
            | Self::TransactionOpenFailed { .. }
            | Self::UserCannotBeRetrieved { .. }
            | Self::UserCannotBeCreated { .. }
            | Self::UserCannotBeUpdated { .. }
            | Self::UserCannotBeDeleted { .. }
            | Self::DatabaseCannotBeCreated { .. }
            | Self::DatabaseCannotBeDeleted { .. }
            | Self::DatabaseExport { .. }
            | Self::DatabaseImport { .. } => InvalidRequest,
        }
    }
}

impl From<LocalServerStateError> for ArcServerStateError {
    fn from(typedb_source: LocalServerStateError) -> Self {
        Arc::new(typedb_source)
    }
}

#[inline]
pub fn arc_server_state_err<E: ServerStateError + Send + Sync + 'static>(e: E) -> ArcServerStateError {
    Arc::new(e)
}
