/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use database::{migration::Checksums, transaction::TransactionRead, Database};
use options::TransactionOptions;
use resource::{constants::common::SECONDS_IN_DAY, distribution_info::DistributionInfo, profile::StorageCounters};
use storage::durability_client::WALClient;
use tokio::sync::{mpsc::Sender, watch};
use tonic::Status;
use tracing::{event, Level};
use typedb_protocol::{database::export::Server as ProtocolServer, migration::Item as MigrationItemProto};

use crate::service::{
    export_service::{get_transaction_schema, DatabaseExportError},
    grpc::{
        error::{IntoGrpcStatus, IntoProtocolErrorMessage},
        migration::item::{
            encode_attribute_item, encode_checksums_item, encode_entity_item, encode_header_item, encode_relation_item,
        },
        response_builders::database::{
            database_export_initial_res_ok, database_export_res_done, database_export_res_part_items,
        },
    },
};

macro_rules! send_response {
    ($response_sender: expr, $message: expr) => {{
        let res = $response_sender.send($message).await;
        if let Err(err) = &res {
            event!(Level::TRACE, "Send database export message failed: {:?}", err);
        }
        res.map_err(|_| DatabaseExportError::ClientChannelIsClosed {})
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

macro_rules! return_error_if_shutdown {
    ($self:ident) => {{
        match $self.shutdown_receiver.has_changed() {
            Ok(true) => return Err(DatabaseExportError::ShutdownInterrupt {}),
            Ok(false) => {}
            Err(err) => {
                // If the channel is closed, something has happened. Log + consider it a shutdown
                event!(Level::TRACE, "Shutdown receiver is not available from export service: {:?}", err);
                return Err(DatabaseExportError::ShutdownInterrupt {});
            }
        }
    }};
}

pub(crate) const DATABASE_EXPORT_REQUEST_BUFFER_SIZE: usize = 10;
const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
pub(crate) struct DatabaseExportService {
    distribution_info: DistributionInfo,
    database: Arc<Database<WALClient>>,
    response_sender: ResponseSender,
    checksums: Checksums,
    shutdown_receiver: watch::Receiver<()>,

    total_item_count: u64,
}

impl DatabaseExportService {
    const ITEM_BATCH_SIZE: usize = 250;

    const OPTIONS_PARALLEL: bool = true;
    const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(1 * SECONDS_IN_DAY).as_millis() as u64;

    pub(crate) fn new(
        distribution_info: DistributionInfo,
        database: Arc<Database<WALClient>>,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        Self {
            distribution_info,
            database,
            response_sender,
            checksums: Checksums::new(),
            shutdown_receiver,
            total_item_count: 0,
        }
    }

    pub(crate) async fn export(mut self) {
        let start = Instant::now();
        event!(Level::INFO, "Exporting '{}' from TypeDB {}.", self.database.name(), self.distribution_info.version);
        let Some(transaction) = self.open_transaction().await else {
            return;
        };

        let schema = unwrap_else_send_error_and_return!(self, get_transaction_schema(&transaction));
        unwrap_else_send_error_and_return!(
            self,
            send_response!(self.response_sender, Ok(database_export_initial_res_ok(schema)))
        );

        let mut buffer = Vec::with_capacity(Self::ITEM_BATCH_SIZE);
        unwrap_else_send_error_and_return!(self, self.export_header(&mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_entities(&transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_relations(&transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_attributes(&transaction, &mut buffer).await);
        unwrap_else_send_error_and_return!(self, self.export_checksums(&mut buffer).await);
        if !buffer.is_empty() {
            unwrap_else_send_error_and_return!(self, Self::send_items(&self.response_sender, buffer).await);
        }

        unwrap_else_send_error_and_return!(self, Self::send_done(&self.response_sender).await);
        event!(
            Level::INFO,
            "Export '{}' from TypeDB {} finished successfully. {} items exported in {} seconds.",
            self.database.name(),
            self.distribution_info.version,
            self.total_item_count,
            start.elapsed().as_secs()
        );
    }

    async fn export_entities(
        &mut self,
        transaction: &TransactionRead<WALClient>,
        buffer: &mut Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        let entities = transaction.thing_manager.get_entities(transaction.snapshot(), StorageCounters::DISABLED);
        for entity in entities {
            return_error_if_shutdown!(self);
            let entity = entity.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            let item = encode_entity_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                entity,
            )
            .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            self.buffer_push(buffer, item);
            self.checksums.entity_count += 1;
            self.flush_buffer_if_needed(buffer).await?;
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
            return_error_if_shutdown!(self);
            let relation = relation.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            let item = encode_relation_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                relation,
            )
            .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            self.buffer_push(buffer, item);
            self.checksums.relation_count += 1;
            self.flush_buffer_if_needed(buffer).await?;
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
            return_error_if_shutdown!(self);
            let attribute = attribute.map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            let item = encode_attribute_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                attribute,
            )
            .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
            self.buffer_push(buffer, item);
            self.checksums.attribute_count += 1;
            self.flush_buffer_if_needed(buffer).await?;
        }
        Ok(())
    }

    async fn export_header(&mut self, buffer: &mut Vec<MigrationItemProto>) -> Result<(), DatabaseExportError> {
        self.buffer_push(
            buffer,
            encode_header_item(self.distribution_info.version.to_string(), self.database.name().to_string()),
        );
        Ok(())
    }

    async fn export_checksums(&mut self, buffer: &mut Vec<MigrationItemProto>) -> Result<(), DatabaseExportError> {
        self.buffer_push(buffer, encode_checksums_item(&self.checksums));
        Ok(())
    }

    async fn flush_buffer_if_needed(
        &mut self,
        buffer: &mut Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        if buffer.len() >= Self::ITEM_BATCH_SIZE {
            Self::send_items(&self.response_sender, buffer.split_off(0)).await?;
        }
        Ok(())
    }

    fn buffer_push(&mut self, buffer: &mut Vec<MigrationItemProto>, item: MigrationItemProto) {
        buffer.push(item);
        self.total_item_count += 1;

        if self.total_item_count % ITEMS_LOG_INTERVAL == 0 {
            event!(Level::INFO, "Processed {} exported items of '{}'...", self.total_item_count, self.database.name());
        }
    }

    async fn send_error(response_sender: &ResponseSender, error: DatabaseExportError) {
        let _ = send_response!(response_sender, Err(error.into_error_message().into_status())).ok();
    }

    async fn send_done(response_sender: &ResponseSender) -> Result<(), DatabaseExportError> {
        send_response!(response_sender, Ok(database_export_res_done()))
    }

    async fn send_items(
        response_sender: &ResponseSender,
        items: Vec<MigrationItemProto>,
    ) -> Result<(), DatabaseExportError> {
        send_response!(response_sender, Ok(database_export_res_part_items(items)))
    }

    async fn open_transaction(&self) -> Option<TransactionRead<WALClient>> {
        match TransactionRead::open(self.database.clone(), Self::transaction_options()) {
            Ok(transaction) => Some(transaction),
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
