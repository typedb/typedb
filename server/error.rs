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
    service::export_service::DatabaseExportError,
};

pub trait ServerStateError: TypeDBError + Send + Sync + Debug + 'static {}

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
        GrpcHttpConflictingAddress(10, "Configuring HTTP and gRPC on the same address {address} is not supported.", address: SocketAddr),
        GrpcServe(11, "Could not serve gRPC on {address}.", address: SocketAddr, source: Arc<tonic::transport::Error>),
        GrpcCouldNotReadTlsCertificate(12, "Could not read TLS certificate from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcCouldNotReadTlsCertificateKey(13, "Could not read TLS certificate key from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcCouldNotReadRootCa(14, "Could not read root CA from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GrpcInvalidRootCa(15, "Invalid root CA for the gRPC server.", source: Arc<io::Error>),
        GrpcTlsFailedConfiguration(16, "Failed to configure TLS for the gRPC server.", source: Arc<tonic::transport::Error>),
        HttpServe(17, "Could not serve HTTP on {address}.", address: SocketAddr, source: Arc<io::Error>),
        HttpCouldNotReadTlsCertificate(18, "Could not read TLS certificate from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpCouldNotReadTlsCertificateKey(19, "Could not read TLS certificate key from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpCouldNotReadRootCa(20, "Could not read root CA from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HttpInvalidRootCa(21, "Invalid root CA for the HTTP server.", source: Arc<RustlsVerifierError>),
        HttpTlsFailedConfiguration(22, "Failed to configure TLS for the HTTP server.", source: Arc<tokio_rustls::rustls::Error>),
        HttpTlsUnsetDefaultCryptoProvider(23, "Failed to install default crypto provider for the HTTP server TLS configuration."),
        HttpTlsPemFileError(24, "Invalid PEM file specified for the HTTP server.", source: Arc<tokio_rustls::rustls::pki_types::pem::Error>),
        ServerState(25, "Invalid server state.", typedb_source: ArcServerStateError),
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
        DatabaseSchemaCommitFailed(17, "Schema commit failed.", typedb_source: SchemaCommitError),
        DatabaseDataCommitFailed(18, "Data commit failed.", typedb_source: DataCommitError),
        NotSupportedByDistribution(19, "Not supported by this distribution: {description}", description: String),
    }
}

impl ServerStateError for LocalServerStateError {}

impl From<LocalServerStateError> for ArcServerStateError {
    fn from(typedb_source: LocalServerStateError) -> Self {
        Arc::new(typedb_source)
    }
}

#[inline]
pub fn arc_server_state_err<E: ServerStateError + Send + Sync + 'static>(e: E) -> ArcServerStateError {
    Arc::new(e)
}
