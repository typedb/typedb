/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, io, net::SocketAddr, sync::Arc};

use concept::{
    error::{ConceptReadError, ConceptWriteError},
    type_::type_manager::type_cache::TypeCacheCreateError,
};
use database::{
    DatabaseDeleteError, DatabaseOpenError,
    database::DatabaseCreateError,
    migration::database_importer::DatabaseImportError,
    transaction::{DataCommitError, SchemaCommitError, TransactionError},
};
use error::{TypeDBError, typedb_error};
use function::FunctionError;
use ir::pipeline::FunctionReadError;
use storage::{StorageCommitError, snapshot::SnapshotError};
use tokio_rustls::rustls::{
    pki_types::pem::Error as RustlsCertError, server::VerifierBuilderError as RustlsVerifierError,
};
use user::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

use crate::{
    authentication::{AuthenticationError, token_manager::TokenManagerError},
    service::{export_service::DatabaseExportError, import_service::DatabaseImportServiceError},
};

pub enum ErrorResponseCategory {
    NotFound,
    Unauthenticated,
    Forbidden,
    NotImplemented,
    Unavailable,
    /// Redirect for authenticated endpoints. The client cannot follow automatically because
    /// authentication tokens are per-server. Produces HTTP 421 Misdirected Request with the
    /// address in the response body. For gRPC, produces Unavailable with REDIRECT metadata.
    Redirect {
        grpc_address: Option<String>,
        http_address: Option<String>,
    },
    InvalidRequest,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorOrigin {
    /// Determined by the request and the server's persisted state: any server in that state, given
    /// that request, fails the same way, so retrying cannot help.
    Request,
    /// This server's environment failed: I/O, disk, the OS, or a local runtime condition such as
    /// contention, capacity, readiness, or shutdown. Another server, or this server at another
    /// time, might well succeed, so retrying can help.
    System,
    /// The server broke its own invariants: an unexpected state or failure that indicates a bug
    /// rather than a bad request or a failed environment.
    Internal,
}

pub trait ServerStateError: TypeDBError + Send + Sync + Debug + 'static {
    fn error_response_category(&self) -> ErrorResponseCategory;

    fn error_origin(&self) -> ErrorOrigin;
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
        AdminServe(29, "Could not serve admin endpoint at '{path}'.", path: String, source: Arc<tonic::transport::Error>),
        AdminSocketPathInUse(30, "Admin socket path '{path}' already exists and is not a socket. Refusing to overwrite.", path: String),
        AdminSocketBind(31, "Could not bind admin Unix socket at '{path}'.", path: String, source: Arc<io::Error>),
        AdminSocketCleanup(32, "Could not remove stale admin socket file at '{path}'.", path: String, source: Arc<io::Error>),
        AdminSocketChmod(33, "Could not set permissions on admin socket file at '{path}'.", path: String, source: Arc<io::Error>),
        AdminPipeBind(34, "Could not create admin Named Pipe '{name}'.", name: String, source: Arc<io::Error>),
        AdminSocketPathTooLong(
            35,
            "Admin socket path '{path}' ({length} bytes) is too long for a Unix domain socket on this OS. Set --server.admin.socket-path to a shorter path (e.g. somewhere under /tmp).",
            path: String,
            length: usize
        ),
        MonitoringConflictingAddress(36, "Configuring diagnostics monitoring on the same address {address} as another service is not supported.", address: SocketAddr),
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
        DatabaseImportPrepareFailed(23, "Unable to prepare database import.", typedb_source: DatabaseCreateError),
        DatabaseImportFinaliseFailed(24, "Unable to finalise database import.", typedb_source: DatabaseCreateError),
        DatabaseImportDiscardFailed(25, "Unable to discard database import.", typedb_source: DatabaseDeleteError),
        ConcurrentImportLimitReached(27, "Too many concurrent imports (limit {limit}). Retry later.", limit: usize),
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

            Self::UserCannotBeUpdated { typedb_source: UserUpdateError::UserNotFound { .. } }
            | Self::UserCannotBeDeleted { typedb_source: UserDeleteError::UserNotFound { .. } } => NotFound,

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
            | Self::DatabaseImport { .. }
            | Self::DatabaseImportPrepareFailed { .. }
            | Self::DatabaseImportFinaliseFailed { .. }
            | Self::DatabaseImportDiscardFailed { .. } => InvalidRequest,
            Self::ConcurrentImportLimitReached { .. } => Unavailable,
        }
    }

    fn error_origin(&self) -> ErrorOrigin {
        use ErrorOrigin::{Internal, Request, System};

        use crate::authentication::AuthenticationError;

        match self {
            Self::Unimplemented { .. }
            | Self::NotSupportedByDistribution { .. }
            | Self::OperationNotPermitted { .. }
            | Self::DatabaseNotFound { .. }
            | Self::UserNotFound { .. } => Request,
            Self::AuthenticationError { typedb_source } => match typedb_source {
                AuthenticationError::InvalidCredential { .. }
                | AuthenticationError::MissingToken { .. }
                | AuthenticationError::InvalidToken { .. } => Request,
                AuthenticationError::CorruptedAccessor { .. } => Internal,
            },
            Self::UserCannotBeRetrieved { typedb_source } => user_get_origin(typedb_source),
            Self::UserCannotBeCreated { typedb_source } => match typedb_source {
                UserCreateError::IllegalUsername { .. }
                | UserCreateError::UserAlreadyExist { .. }
                | UserCreateError::IncompleteUserDetail { .. } => Request,
                UserCreateError::GetFailed { typedb_source } => user_get_origin(typedb_source),
                UserCreateError::Unexpected { .. } => Internal,
            },
            Self::UserCannotBeUpdated { typedb_source } => match typedb_source {
                UserUpdateError::UserDetailNotProvided { .. }
                | UserUpdateError::IllegalUsername { .. }
                | UserUpdateError::UserNotFound { .. } => Request,
                UserUpdateError::GetFailed { typedb_source } => user_get_origin(typedb_source),
                UserUpdateError::Unexpected { .. } => Internal,
            },
            Self::UserCannotBeDeleted { typedb_source } => match typedb_source {
                UserDeleteError::DefaultUserCannotBeDeleted { .. }
                | UserDeleteError::IllegalUsername { .. }
                | UserDeleteError::UserNotFound { .. } => Request,
                UserDeleteError::GetFailed { typedb_source } => user_get_origin(typedb_source),
                UserDeleteError::Unexpected { .. } => Internal,
            },
            Self::DatabaseCannotBeCreated { typedb_source } => database_create_origin(typedb_source),
            Self::DatabaseCannotBeDeleted { typedb_source } => database_delete_origin(typedb_source),
            Self::DatabaseImportPrepareFailed { typedb_source } => database_create_origin(typedb_source),
            Self::DatabaseImportFinaliseFailed { typedb_source } => database_create_origin(typedb_source),
            Self::DatabaseImportDiscardFailed { typedb_source } => database_delete_origin(typedb_source),
            Self::DatabaseSchemaCommitFailed { typedb_source } => match typedb_source {
                SchemaCommitError::ConceptWriteErrors { write_errors, .. } => {
                    concept_writes_origin(write_errors.iter())
                }
                SchemaCommitError::ConceptWriteErrorsFirst { typedb_source, .. } => concept_write_origin(typedb_source),
                SchemaCommitError::SnapshotError { typedb_source, .. } => snapshot_commit_origin(typedb_source),
                SchemaCommitError::FunctionError { typedb_source } => match typedb_source {
                    FunctionError::FunctionNotFound { .. }
                    | FunctionError::AllFunctionsTypeCheckFailure { .. }
                    | FunctionError::CommittedFunctionsTypeCheck { .. }
                    | FunctionError::FunctionTranslation { .. }
                    | FunctionError::FunctionAlreadyExists { .. }
                    | FunctionError::CommittedFunctionParseError { .. }
                    | FunctionError::StratificationViolation { .. } => Request,
                    FunctionError::ConceptRead { typedb_source } => concept_read_origin(typedb_source),
                    FunctionError::CreateFunctionEncoding { .. } => Internal,
                    FunctionError::FunctionRetrieval { typedb_source } => function_read_origin(typedb_source),
                },
                SchemaCommitError::TypeCacheUpdateError { typedb_source } => match typedb_source {
                    TypeCacheCreateError::Empty { .. } => Internal,
                    TypeCacheCreateError::SnapshotIterate { .. } => System,
                },
                SchemaCommitError::StatisticsError { .. } => System,
            },
            Self::DatabaseDataCommitFailed { typedb_source } => match typedb_source {
                DataCommitError::ConceptWriteErrors { write_errors, .. } => concept_writes_origin(write_errors.iter()),
                DataCommitError::ConceptWriteErrorsFirst { typedb_source, .. } => concept_write_origin(typedb_source),
                DataCommitError::SnapshotError { typedb_source, .. } => snapshot_commit_origin(typedb_source),
                DataCommitError::SnapshotInUse { .. } => Internal,
            },
            Self::DatabaseImport { typedb_source } => match typedb_source {
                DatabaseImportServiceError::ConceptDecode { .. }
                | DatabaseImportServiceError::DuplicateImport { .. }
                | DatabaseImportServiceError::ImportDatabaseNotFound { .. }
                | DatabaseImportServiceError::ImportEmptyItem { .. }
                | DatabaseImportServiceError::AbsentAttributeValue { .. }
                | DatabaseImportServiceError::AttributesOwningAttributes { .. }
                | DatabaseImportServiceError::ClientClosed { .. } => Request,
                DatabaseImportServiceError::ImportPrepareFailed { typedb_source } => typedb_source.error_origin(),
                DatabaseImportServiceError::DatabaseImport { typedb_source } => database_import_origin(typedb_source),
                DatabaseImportServiceError::ImportTaskFailed { .. } => Internal,
                DatabaseImportServiceError::ImportClosed { .. }
                | DatabaseImportServiceError::ShutdownInterrupt { .. } => System,
            },
            Self::ConceptReadError { typedb_source } => concept_read_origin(typedb_source),
            Self::FunctionReadError { typedb_source } => function_read_origin(typedb_source),
            Self::DatabaseExport { typedb_source } => match typedb_source {
                DatabaseExportError::ClientChannelIsClosed { .. } => Request,
                DatabaseExportError::ConceptRead { typedb_source } => concept_read_origin(typedb_source),
                DatabaseExportError::FunctionRead { typedb_source } => function_read_origin(typedb_source),
                DatabaseExportError::ShutdownInterrupt { .. }
                | DatabaseExportError::TransactionCloseInterrupt { .. } => System,
            },
            Self::NotInitialised { .. }
            | Self::FailedToOpenPrerequisiteTransaction { .. }
            | Self::TransactionOpenFailed { .. }
            | Self::DatabaseCommitRecordExistsFailed { .. }
            | Self::ConcurrentImportLimitReached { .. } => System,
        }
    }
}

fn user_get_origin(error: &UserGetError) -> ErrorOrigin {
    match error {
        UserGetError::IllegalUsername { .. } => ErrorOrigin::Request,
        UserGetError::Unexpected { .. } => ErrorOrigin::Internal,
    }
}

fn database_create_origin(error: &DatabaseCreateError) -> ErrorOrigin {
    match error {
        DatabaseCreateError::InvalidName { .. }
        | DatabaseCreateError::InternalDatabaseCreationProhibited { .. }
        | DatabaseCreateError::AlreadyExists { .. }
        | DatabaseCreateError::IsBeingImported { .. } => ErrorOrigin::Request,

        DatabaseCreateError::DatabaseOpen { .. }
        | DatabaseCreateError::WriteAccessDenied { .. }
        | DatabaseCreateError::ReadAccessDenied { .. }
        | DatabaseCreateError::AlreadyExistsAndCleanupBlocked { .. }
        | DatabaseCreateError::DirectoryWrite { .. }
        | DatabaseCreateError::DatabaseMove { .. }
        | DatabaseCreateError::ImportCleanupFailed { .. } => ErrorOrigin::System,

        DatabaseCreateError::IsNotBeingImported { .. } | DatabaseCreateError::ImportedDatabaseInUse { .. } => {
            ErrorOrigin::Internal
        }
    }
}

fn database_delete_origin(error: &DatabaseDeleteError) -> ErrorOrigin {
    match error {
        DatabaseDeleteError::DoesNotExist { .. }
        | DatabaseDeleteError::InternalDatabaseDeletionProhibited { .. }
        | DatabaseDeleteError::DatabaseIsNotBeingImported { .. } => ErrorOrigin::Request,

        DatabaseDeleteError::InUse { .. }
        | DatabaseDeleteError::StorageDelete { .. }
        | DatabaseDeleteError::DirectoryDelete { .. }
        | DatabaseDeleteError::WriteAccessDenied { .. } => ErrorOrigin::System,
    }
}

fn database_import_origin(error: &DatabaseImportError) -> ErrorOrigin {
    match error {
        DatabaseImportError::SchemaQueryParseFailed { .. }
        | DatabaseImportError::SchemaQueryFailed { .. }
        | DatabaseImportError::InvalidSchemaDefineQuery { .. }
        | DatabaseImportError::ProvidedSchemaCommitFailed { .. }
        | DatabaseImportError::DuplicateClientChecksums { .. }
        | DatabaseImportError::UnknownAttributeType { .. }
        | DatabaseImportError::UnknownEntityType { .. }
        | DatabaseImportError::UnknownRelationType { .. }
        | DatabaseImportError::UnknownRoleType { .. }
        | DatabaseImportError::NoClientChecksumsOnDone { .. }
        | DatabaseImportError::InvalidChecksumsOnDone { .. }
        | DatabaseImportError::IncompleteOwnershipsOnDone { .. }
        | DatabaseImportError::IncompleteRolesOnDone { .. }
        | DatabaseImportError::DoubleFinalisation { .. }
        | DatabaseImportError::AccessAfterFinalisation { .. } => ErrorOrigin::Request,

        DatabaseImportError::TransactionFailed { .. }
        | DatabaseImportError::DataCommitFailed { .. }
        | DatabaseImportError::FinalisationFailed { .. }
        | DatabaseImportError::CacheError { .. }
        | DatabaseImportError::Interrupted { .. } => ErrorOrigin::System,

        DatabaseImportError::PreparationSchemaCommitFailed { .. }
        | DatabaseImportError::FinalizationSchemaCommitFailed { .. } => ErrorOrigin::Internal,

        DatabaseImportError::ConceptRead { typedb_source } => concept_read_origin(typedb_source),
        DatabaseImportError::ConceptWrite { typedb_source } => concept_write_origin(typedb_source),
    }
}

fn concept_writes_origin<'a>(errors: impl Iterator<Item = &'a ConceptWriteError>) -> ErrorOrigin {
    errors.map(concept_write_origin).fold(ErrorOrigin::Request, combined_origin)
}

fn combined_origin(first: ErrorOrigin, second: ErrorOrigin) -> ErrorOrigin {
    match (first, second) {
        (ErrorOrigin::Internal, _) | (_, ErrorOrigin::Internal) => ErrorOrigin::Internal,
        (ErrorOrigin::System, _) | (_, ErrorOrigin::System) => ErrorOrigin::System,
        (ErrorOrigin::Request, ErrorOrigin::Request) => ErrorOrigin::Request,
    }
}

fn concept_write_origin(error: &ConceptWriteError) -> ErrorOrigin {
    match error {
        ConceptWriteError::SchemaValidation { .. }
        | ConceptWriteError::DataValidation { .. }
        | ConceptWriteError::Annotation { .. }
        | ConceptWriteError::SetHasOrderedOwnsUnordered { .. }
        | ConceptWriteError::SetHasUnorderedOwnsOrdered { .. }
        | ConceptWriteError::UnsetHasOrderedOwnsUnordered { .. }
        | ConceptWriteError::UnsetHasUnorderedOwnsOrdered { .. }
        | ConceptWriteError::SetPlayersOrderedRoleUnordered { .. } => ErrorOrigin::Request,

        ConceptWriteError::SnapshotGet { .. } | ConceptWriteError::SnapshotIterate { .. } => ErrorOrigin::System,

        ConceptWriteError::Encoding { .. } => ErrorOrigin::Internal,

        ConceptWriteError::ConceptRead { typedb_source } => concept_read_origin(typedb_source),
    }
}

fn concept_read_origin(error: &ConceptReadError) -> ErrorOrigin {
    match error {
        ConceptReadError::SnapshotGet { .. }
        | ConceptReadError::SnapshotIterate { .. }
        | ConceptReadError::LoadSchemaSnapshot { .. } => ErrorOrigin::System,
        _ => ErrorOrigin::Internal,
    }
}

fn function_read_origin(error: &FunctionReadError) -> ErrorOrigin {
    match error {
        FunctionReadError::FunctionRetrieval { .. } | FunctionReadError::FunctionsScan { .. } => ErrorOrigin::System,
        FunctionReadError::FunctionIDNotFound { .. } | FunctionReadError::FormatError { .. } => ErrorOrigin::Internal,
    }
}

fn snapshot_commit_origin(error: &SnapshotError) -> ErrorOrigin {
    match error {
        SnapshotError::Commit { typedb_source, .. } => match typedb_source {
            StorageCommitError::Isolation { .. } => ErrorOrigin::Request,
            StorageCommitError::IO { .. }
            | StorageCommitError::MVCCRead { .. }
            | StorageCommitError::Keyspace { .. }
            | StorageCommitError::Durability { .. } => ErrorOrigin::System,
            StorageCommitError::Internal { .. } => ErrorOrigin::Internal,
        },
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
