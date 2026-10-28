/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Duration};

use concept::error::ConceptDecodeError;
use database::transaction::{CommitIntent, TransactionError, TransactionSchema, TransactionWrite};
use error::typedb_error;
use executor::{InterruptType, pipeline::PipelineExecutionError};
use query::error::QueryError;
use resource::{constants::server::DEFAULT_TRANSACTION_TIMEOUT_MILLIS, profile::TransactionProfile};
use storage::durability_client::WALClient;
use tokio::time::Instant;
use tracing::{Level, event};
use typeql::query::stage::Stage;
use uuid::Uuid;

pub(crate) use crate::transaction::{Transaction, with_readable_transaction};
use crate::{
    error::{ArcServerStateError, LocalServerStateError},
    state::ServerState,
};

pub(crate) const TRANSACTION_REQUEST_BUFFER_SIZE: usize = 10;

pub(crate) fn check_vector_definitions_enabled(
    schema_query: &typeql::query::SchemaQuery,
    is_vector_search_enabled: bool,
) -> Result<(), TransactionServiceError> {
    use typeql::{
        query::SchemaQuery,
        schema::definable::Definable,
        type_::{NamedType, NamedTypeAny, NamedTypeList, NamedTypeOptional},
    };
    if is_vector_search_enabled {
        return Ok(());
    }
    let definables = match schema_query {
        SchemaQuery::Define(define) => &define.definables,
        SchemaQuery::Redefine(redefine) => &redefine.definables,
        SchemaQuery::Undefine(_) => return Ok(()),
    };
    fn is_vector(named: &NamedTypeAny) -> bool {
        let (NamedTypeAny::Simple(inner)
        | NamedTypeAny::List(NamedTypeList { inner, .. })
        | NamedTypeAny::Optional(NamedTypeOptional { inner, .. })) = named;
        matches!(inner, NamedType::Vector(_))
    }
    let defines_vector = definables.iter().any(|definable| match definable {
        Definable::TypeDeclaration(declaration) => declaration.capabilities.iter().any(|capability| {
            matches!(
                &capability.base,
                typeql::schema::definable::type_::CapabilityBase::ValueType(value_type)
                    if matches!(&value_type.value_type, NamedType::Vector(_))
            )
        }),
        Definable::Struct(struct_) => struct_.fields.iter().any(|field| is_vector(&field.type_)),
        Definable::Function(_) => false,
        Definable::TypeRename(_) | Definable::RoleTypeRename(_) => false,
    });
    if defines_vector { Err(TransactionServiceError::VectorSearchNotEnabled {}) } else { Ok(()) }
}

pub(crate) fn is_write_pipeline(pipeline: &typeql::query::Pipeline) -> bool {
    for stage in &pipeline.stages {
        match stage {
            Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => return true,
            Stage::Given(_) | Stage::Fetch(_) | Stage::Operator(_) | Stage::Match(_) => {}
        }
    }
    false
}

pub(crate) fn init_transaction_timeout(transaction_timeout_millis: Option<u64>) -> Instant {
    Instant::now() + Duration::from_millis(transaction_timeout_millis.unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_MILLIS))
}

pub(crate) async fn commit_schema_transaction(
    server_state: Arc<ServerState>,
    transaction: TransactionSchema<WALClient>,
) -> (TransactionProfile, Result<(), ArcServerStateError>) {
    let (mut profile, result) = match transaction.finalise() {
        (mut profile, Ok(commit_intent)) => {
            if !commit_intent.has_changes() {
                (profile, Ok(()))
            } else {
                let commit_profile = profile.take_commit_profile();
                let (commit_profile, result) =
                    server_state.databases().schema_commit(commit_intent, commit_profile).await;
                profile.set_commit_profile(commit_profile);
                (profile, result)
            }
        }
        (profile, Err(typedb_source)) => {
            (profile, Err(LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source }.into()))
        }
    };
    profile.commit_profile().end();
    if profile.is_enabled() {
        event!(Level::INFO, "schema commit done.\n{}", profile);
    }
    (profile, result)
}

pub(crate) async fn commit_write_transaction(
    server_state: Arc<ServerState>,
    transaction: TransactionWrite<WALClient>,
) -> (TransactionProfile, Result<(), ArcServerStateError>) {
    let (mut profile, result) = match transaction.finalise() {
        (mut profile, Ok(commit_intent)) => {
            if !commit_intent.has_changes() {
                (profile, Ok(()))
            } else {
                let commit_profile = profile.take_commit_profile();
                let (commit_profile, result) =
                    server_state.databases().data_commit(commit_intent, commit_profile).await;
                profile.set_commit_profile(commit_profile);
                (profile, result)
            }
        }
        (profile, Err(typedb_source)) => {
            (profile, Err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source }.into()))
        }
    };
    profile.commit_profile().end();
    if profile.is_enabled() {
        event!(Level::INFO, "data commit done.\n{}", profile);
    }
    (profile, result)
}

typedb_error! {
    pub TransactionServiceError(component = "Transaction service", prefix = "TSV") {
        DatabaseNotFound(1, "Database '{name}' not found.", name: String),
        CannotCommitReadTransaction(2, "Read transactions cannot be committed."),
        CannotRollbackReadTransaction(3, "Read transactions cannot be rolled back, since they never contain writes."),
        TransactionFailed(4, "Transaction failed.", typedb_source: TransactionError),
        DataCommitFailed(5, "Data transaction commit failed.", typedb_source: ArcServerStateError),
        SchemaCommitFailed(6, "Schema transaction commit failed.", typedb_source: ArcServerStateError),
        QueryParseFailed(7, "Query parsing failed.", typedb_source: typeql::Error),
        SchemaQueryRequiresSchemaTransaction(8, "Schema modification queries require schema transactions."),
        WriteQueryRequiresSchemaOrWriteTransaction(9, "Data modification queries require either write or schema transactions."),
        SchemaQueryFailedAbortingTransaction(10, "Aborting transaction due to failed schema query.", typedb_source: Box<QueryError>),
        QueryFailed(11, "Query failed.", typedb_source: Box<QueryError>),
        NoOpenTransaction(12, "Operation failed: no open transaction."),
        QueryInterrupted(13, "Execution interrupted by a concurrent {interrupt}.", interrupt: InterruptType),
        QueryStreamNotFound(
            14,
            r#"
            Query stream with id '{query_request_id}' was not found in the transaction.
            The stream could have already finished, or the transaction could be closed, committed, rolled back (or this is a bug).
            "#,
            query_request_id: Uuid
        ),
        QueueCleanupFailed(15, "The operation failed since the service is closing."),
        PipelineExecution(16, "Pipeline execution failed.", typedb_source: PipelineExecutionError),
        TransactionTimeout(17, "Operation failed: transaction timeout."),
        InvalidPrefetchSize(18, "Invalid query option: prefetch size should be >= 1, got {value} instead.", value: usize),
        AnalyseQueryExpectsPipeline(19, "Query analyse received a schema query. Only query pipelines can be analysed."),
        AnalyseQueryFailed(20, "Analysing the query failed.", typedb_source: QueryError),
        CannotOpen(21, "Could not open transaction.", typedb_source: ArcServerStateError),
        DecodingGivenRowsFailed(22, "Decoding the input failed", typedb_source: ConceptDecodeError),
        WriteQueryAnswersInterrupted(
            23,
            "Answer stream interrupted by a concurrent {interrupt}. The write was still applied; only its answers were dropped.",
            interrupt: InterruptType
        ),
        VectorSearchNotEnabled(24, "Vector search is not enabled. Start the server with '--experimental-vector-search-enabled=true' to enable this experimental functionality."),
    }
}

impl TransactionServiceError {
    pub fn to_service_error(&self) -> Option<&ArcServerStateError> {
        match self {
            TransactionServiceError::DatabaseNotFound { .. }
            | TransactionServiceError::CannotCommitReadTransaction { .. }
            | TransactionServiceError::CannotRollbackReadTransaction { .. }
            | TransactionServiceError::TransactionFailed { .. }
            | TransactionServiceError::QueryParseFailed { .. }
            | TransactionServiceError::SchemaQueryRequiresSchemaTransaction { .. }
            | TransactionServiceError::WriteQueryRequiresSchemaOrWriteTransaction { .. }
            | TransactionServiceError::SchemaQueryFailedAbortingTransaction { .. }
            | TransactionServiceError::QueryFailed { .. }
            | TransactionServiceError::NoOpenTransaction { .. }
            | TransactionServiceError::QueryInterrupted { .. }
            | TransactionServiceError::WriteQueryAnswersInterrupted { .. }
            | TransactionServiceError::QueryStreamNotFound { .. }
            | TransactionServiceError::QueueCleanupFailed { .. }
            | TransactionServiceError::PipelineExecution { .. }
            | TransactionServiceError::TransactionTimeout { .. }
            | TransactionServiceError::InvalidPrefetchSize { .. }
            | TransactionServiceError::AnalyseQueryExpectsPipeline { .. }
            | TransactionServiceError::AnalyseQueryFailed { .. }
            | TransactionServiceError::DecodingGivenRowsFailed { .. }
            | TransactionServiceError::VectorSearchNotEnabled { .. } => None,

            TransactionServiceError::DataCommitFailed { typedb_source }
            | TransactionServiceError::SchemaCommitFailed { typedb_source }
            | TransactionServiceError::CannotOpen { typedb_source } => Some(typedb_source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::check_vector_definitions_enabled;

    fn schema_query(query: &str) -> typeql::query::SchemaQuery {
        let typeql::query::QueryStructure::Schema(schema_query) = typeql::parse_query(query).unwrap().into_structure()
        else {
            panic!("expected schema query")
        };
        schema_query
    }

    #[test]
    fn vector_definitions_are_rejected_only_when_disabled() {
        let vector_define = schema_query(r#"define attribute embedding, value vector(3, "float32");"#);
        let vector_struct_define =
            schema_query(r#"define struct doc: content value string, vec value vector(3, "float32");"#);
        let vector_redefine = schema_query(r#"redefine attribute embedding value vector(4, "float32");"#);
        let plain_define = schema_query("define attribute name, value string; entity person owns name;");
        let undefine = schema_query("undefine embedding;");

        assert!(check_vector_definitions_enabled(&vector_define, false).is_err());
        assert!(check_vector_definitions_enabled(&vector_struct_define, false).is_err());
        assert!(check_vector_definitions_enabled(&vector_redefine, false).is_err());
        assert!(check_vector_definitions_enabled(&plain_define, false).is_ok());
        assert!(check_vector_definitions_enabled(&undefine, false).is_ok());

        assert!(check_vector_definitions_enabled(&vector_define, true).is_ok());
        assert!(check_vector_definitions_enabled(&vector_redefine, true).is_ok());
    }
}
