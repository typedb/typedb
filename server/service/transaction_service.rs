/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Duration};

use database::transaction::{TransactionError, TransactionSchema, TransactionWrite};
use error::typedb_error;
use executor::{pipeline::PipelineExecutionError, InterruptType};
use query::error::QueryError;
use resource::{constants::server::DEFAULT_TRANSACTION_TIMEOUT_MILLIS, profile::TransactionProfile};
use storage::durability_client::WALClient;
use tokio::time::Instant;
use typeql::query::stage::Stage;
use uuid::Uuid;

pub(crate) use crate::transaction::{with_readable_transaction, Transaction};
use crate::{
    error::{ArcServerStateError, LocalServerStateError},
    state::ServerState,
};

pub(crate) const TRANSACTION_REQUEST_BUFFER_SIZE: usize = 10;

pub(crate) fn is_write_pipeline(pipeline: &typeql::query::Pipeline) -> bool {
    for stage in &pipeline.stages {
        match stage {
            Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => return true,
            Stage::Fetch(_) | Stage::Operator(_) | Stage::Match(_) => {}
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
    match transaction.finalise() {
        (mut profile, Ok(commit_intent)) => {
            if !commit_intent.has_changes() {
                return (profile, Ok(()));
            }
            let commit_result =
                server_state.databases().database_schema_commit(commit_intent, profile.commit_profile()).await;
            (profile, commit_result)
        }
        (profile, Err(typedb_source)) => {
            (profile, Err(LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source }.into()))
        }
    }
}

pub(crate) async fn commit_write_transaction(
    server_state: Arc<ServerState>,
    transaction: TransactionWrite<WALClient>,
) -> (TransactionProfile, Result<(), ArcServerStateError>) {
    match transaction.finalise() {
        (mut profile, Ok(commit_intent)) => {
            if !commit_intent.has_changes() {
                return (profile, Ok(()));
            }
            let commit_result =
                server_state.databases().database_data_commit(commit_intent, profile.commit_profile()).await;
            (profile, commit_result)
        }
        (profile, Err(typedb_source)) => {
            (profile, Err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source }.into()))
        }
    }
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
        TxnAbortSchemaQueryFailed(10, "Aborting transaction due to failed schema query.", typedb_source: QueryError),
        QueryFailed(11, "Query failed.", typedb_source: Box<QueryError>),
        NoOpenTransaction(12, "Operation failed: no open transaction."),
        QueryInterrupted(13, "Execution interrupted by to a concurrent {interrupt}.", interrupt: InterruptType),
        QueryStreamNotFound(
            14,
            r#"
            Query stream with id '{query_request_id}' was not found in the transaction.
            The stream could have already finished, or the transaction could be closed, committed, rolled back (or this is a bug).
            "#,
            query_request_id: Uuid
        ),
        ServiceFailedQueueCleanup(15, "The operation failed since the service is closing."),
        PipelineExecution(16, "Pipeline execution failed.", typedb_source: PipelineExecutionError),
        TransactionTimeout(17, "Operation failed: transaction timeout."),
        InvalidPrefetchSize(18, "Invalid query option: prefetch size should be >= 1, got {value} instead.", value: usize),
        AnalyseQueryExpectsPipeline(19, "Query analyse received a schema query.Only query pipeline can be analysed."),
        AnalyseQueryFailed(20, "Analysing the query failed.", typedb_source: QueryError),
        CannotOpen(21, "Could not open transaction.", typedb_source: ArcServerStateError),
    }
}
