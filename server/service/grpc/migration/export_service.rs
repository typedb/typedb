/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, VecDeque},
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Duration,
};

use compiler::query_structure::QueryStructure;
use concept::{
    error::ConceptReadError,
    thing::{entity::Entity, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionError, TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use diagnostics::{
    diagnostics_manager::DiagnosticsManager,
    metrics::{ActionKind, ClientEndpoint, LoadKind},
};
use error::typedb_error;
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::{pipeline::Pipeline, stage::ReadPipelineStage, PipelineExecutionError},
    ExecutionInterrupt, InterruptType,
};
use ir::pipeline::ParameterRegistry;
use itertools::{Either, Itertools};
use lending_iterator::LendingIterator;
use options::{QueryOptions, TransactionOptions};
use query::{error::QueryError, query_manager::QueryManager};
use resource::{
    constants::{
        common::{SECONDS_IN_HOUR, SECONDS_IN_MINUTE},
        server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS},
    },
    profile::{EncodingProfile, QueryProfile, StorageCounters},
    server_info::ServerInfo,
};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadSnapshot, ReadableSnapshot},
};
use tokio::{
    sync::{
        broadcast,
        mpsc::{channel, Receiver, Sender},
        watch,
    },
    task::{spawn_blocking, JoinHandle},
    time::{timeout, Instant},
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{database::export::Server as ProtocolServer, migration::Item as MigrationItemProto};
use typeql::{parse_query, query::SchemaQuery, Query};
use uuid::Uuid;

use crate::service::{
    export_service::{get_transaction_schema, DatabaseExportError},
    grpc::{
        diagnostics::run_with_diagnostics_async,
        document::encode_document,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
        migration::{
            item::{
                encode_attribute_item, encode_checksums_item, encode_entity_item, encode_header_item,
                encode_relation_item,
            },
            Checksums, TransactionHolder,
        },
        options::{query_options_from_proto, transaction_options_from_proto},
        response_builders::{
            database::{database_export_initial_res_ok, database_export_res_done, database_export_res_part_items},
            transaction::{
                query_initial_res_from_error, query_initial_res_from_query_res_ok,
                query_initial_res_ok_from_query_res_ok_ok, query_res_ok_concept_document_stream,
                query_res_ok_concept_row_stream, query_res_ok_done, query_res_part_from_concept_documents,
                query_res_part_from_concept_rows, transaction_open_res,
                transaction_server_res_part_stream_signal_continue, transaction_server_res_part_stream_signal_done,
                transaction_server_res_part_stream_signal_error, transaction_server_res_parts_query_part,
                transaction_server_res_query_res, transaction_server_res_rollback_res,
            },
        },
        row::encode_row,
    },
    transaction_service::{
        execute_schema_query, execute_write_query_in_schema, execute_write_query_in_write, init_transaction_timeout,
        is_write_pipeline, prepare_read_query_in, unwrap_or_execute_and_return, with_readable_transaction,
        StreamQueryOutputDescriptor, Transaction, TransactionServiceError, WriteQueryAnswer, WriteQueryResult,
    },
};

macro_rules! send_response {
    ($response_sender: expr, $message: expr) => {{
        match $response_sender.send($message).await {
            Ok(_) => true,
            Err(err) => {
                event!(Level::TRACE, "Submit database export message failed: {:?}", err);
                false
            }
        }
    }};
}

macro_rules! unwrap_else_send_error_and_return {
    ($self:ident, $expr:expr) => {{
        match $expr {
            Ok(result) => result,
            Err(error) => {
                Self::send_error(&$self.response_sender, error).await;
                return;
            }
        }
    }};
}

pub(crate) const DATABASE_EXPORT_REQUEST_BUFFER_SIZE: usize = 10;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
pub(crate) struct DatabaseExportService {
    server_info: ServerInfo,
    database: Arc<Database<WALClient>>,
    response_sender: ResponseSender,
    checksums: Checksums,
    shutdown_receiver: watch::Receiver<()>,
}

impl DatabaseExportService {
    const ITEM_BATCH_SIZE: usize = 250;

    const OPTIONS_PARALLEL: bool = true;
    const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(6 * SECONDS_IN_HOUR).as_millis() as u64;

    pub(crate) fn new(
        server_info: ServerInfo,
        database: Arc<Database<WALClient>>,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        Self { server_info, database, response_sender, checksums: Checksums::new(), shutdown_receiver }
    }

    pub(crate) async fn export(mut self) {
        let Some(holder) = self.open_transaction().await else {
            return;
        };
        let transaction = holder.transaction();

        let schema = unwrap_else_send_error_and_return!(self, get_transaction_schema(transaction));
        if !send_response!(self.response_sender, Ok(database_export_initial_res_ok(schema))) {
            return;
        }

        let mut buffer = Vec::with_capacity(Self::ITEM_BATCH_SIZE);
        unwrap_else_send_error_and_return!(self, self.export_header(&mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_entities(transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_relations(transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_attributes(transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_checksums(&mut buffer).await);
        if !buffer.is_empty() {
            Self::send_items(&self.response_sender, buffer).await;
        }

        Self::send_done(&self.response_sender).await;
    }

    async fn export_entities(
        &mut self,
        transaction: &TransactionRead<WALClient>,
        buffer: &mut Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        let entities = transaction.thing_manager.get_entities(transaction.snapshot(), StorageCounters::DISABLED);
        for entity in entities {
            let entity = entity.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            buffer.push(
                encode_entity_item(
                    transaction.snapshot(),
                    &transaction.type_manager,
                    &transaction.thing_manager,
                    &mut self.checksums,
                    entity,
                )
                .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?,
            );
            self.checksums.entity_count += 1;
            self.flush_buffer_if_needed(buffer).await;
        }
        Ok(())
    }

    async fn export_relations(
        &mut self,
        transaction: &TransactionRead<WALClient>,
        buffer: &mut Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        let relations = transaction.thing_manager.get_relations(transaction.snapshot(), StorageCounters::DISABLED);
        for relation in relations {
            let relation = relation.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            buffer.push(
                encode_relation_item(
                    transaction.snapshot(),
                    &transaction.type_manager,
                    &transaction.thing_manager,
                    &mut self.checksums,
                    relation,
                )
                .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?,
            );
            self.checksums.relation_count += 1;
            self.flush_buffer_if_needed(buffer).await;
        }
        Ok(())
    }

    async fn export_attributes(
        &mut self,
        transaction: &TransactionRead<WALClient>,
        buffer: &mut Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        let attributes = transaction
            .thing_manager
            .get_attributes(transaction.snapshot(), StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
        for attribute in attributes {
            let attribute = attribute.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            buffer.push(
                encode_attribute_item(
                    transaction.snapshot(),
                    &transaction.type_manager,
                    &transaction.thing_manager,
                    attribute,
                )
                .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?,
            );
            self.checksums.attribute_count += 1;
            self.flush_buffer_if_needed(buffer).await;
        }
        Ok(())
    }

    async fn export_header(&mut self, buffer: &mut Vec<MigrationItemProto>) -> Result<(), DatabaseExportError> {
        buffer.push(encode_header_item(self.server_info.version.to_string(), self.database.name().to_string()));
        Ok(())
    }

    async fn export_checksums(&mut self, buffer: &mut Vec<MigrationItemProto>) -> Result<(), DatabaseExportError> {
        buffer.push(encode_checksums_item(&self.checksums));
        Ok(())
    }

    async fn flush_buffer_if_needed(&mut self, buffer: &mut Vec<MigrationItemProto>) {
        if buffer.len() >= Self::ITEM_BATCH_SIZE {
            Self::send_items(&self.response_sender, buffer.split_off(0)).await;
        }
    }

    async fn send_error(response_sender: &ResponseSender, error: DatabaseExportError) {
        let _ = send_response!(response_sender, Err(error.into_error_message().into_status()));
    }

    async fn send_done(response_sender: &ResponseSender) {
        let _ = send_response!(response_sender, Ok(database_export_res_done()));
    }

    async fn send_items(response_sender: &ResponseSender, items: Vec<MigrationItemProto>) {
        let _ = send_response!(response_sender, Ok(database_export_res_part_items(items)));
    }

    async fn open_transaction(&self) -> Option<TransactionHolder> {
        match TransactionRead::open(self.database.clone(), Self::transaction_options()) {
            Ok(transaction) => Some(TransactionHolder::new(transaction)),
            Err(typedb_source) => {
                Self::send_error(&self.response_sender, DatabaseExportError::TransactionFailed { typedb_source }).await;
                None
            }
        }
    }

    fn transaction_options() -> TransactionOptions {
        TransactionOptions {
            parallel: Self::OPTIONS_PARALLEL,
            schema_lock_acquire_timeout_millis: Self::OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
            transaction_timeout_millis: Self::OPTIONS_TRANSACTION_TIMEOUT_MILLIS,
        }
    }
}
