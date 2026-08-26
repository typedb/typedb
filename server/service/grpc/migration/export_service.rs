/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use database::{migration::database_exporter::DatabaseExporter, transaction::TransactionRead};
use options::TransactionOptions;
use resource::{constants::common::SECONDS_IN_DAY, distribution_info::DistributionInfo};
use storage::durability_client::WALClient;
use tokio::sync::{
    mpsc::{Receiver, Sender, error::TrySendError},
    watch,
};
use tonic::Status;
use tracing::{Level, event};
use typedb_protocol::{
    database::export::Server as ProtocolServer,
    migration::{Item as MigrationItemProto, item::Item as MigrationItem},
};

use crate::{
    error::LocalServerStateError,
    service::{
        TransactionType,
        export_service::DatabaseExportError,
        grpc::{
            error::IntoGrpcStatus,
            response_builders::database::{
                database_export_initial_res_ok, database_export_res_done, database_export_res_part_items,
            },
        },
        migration::item::{EncodedItem, encode_item},
    },
    state::ServerState,
    transaction::Transaction,
};

macro_rules! unwrap_else_send_error_and_return {
    ($self:ident, $expr:expr) => {{
        match $expr {
            Ok(result) => result,
            Err(error) => {
                Self::send_error(&$self.response_sender, error);
                return;
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
    server_state: Arc<ServerState>,
    database_name: String,
    owner: String,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,
    close_receiver: Receiver<()>,
    close_sender: Sender<()>,

    total_item_count: u64,
}

impl DatabaseExportService {
    const ITEM_BATCH_SIZE: usize = 250;

    const OPTIONS_PARALLEL: bool = true;
    const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(1 * SECONDS_IN_DAY).as_millis() as u64;

    pub(crate) fn new(
        distribution_info: DistributionInfo,
        server_state: Arc<ServerState>,
        database_name: String,
        owner: String,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        let (close_sender, close_receiver) = tokio::sync::mpsc::channel(1);
        Self {
            distribution_info,
            server_state,
            database_name,
            owner,
            response_sender,
            shutdown_receiver,
            close_receiver,
            close_sender,
            total_item_count: 0,
        }
    }

    pub(crate) async fn export(mut self) {
        let start = Instant::now();
        event!(Level::DEBUG, "Exporting '{}' from TypeDB {}.", self.database_name, self.distribution_info.version);
        let Some(transaction) = self.open_transaction().await else {
            return;
        };

        let mut exporter = unwrap_else_send_error_and_return!(
            self,
            DatabaseExporter::new(&transaction, self.distribution_info.version.to_string(), self.database_name.clone())
                .map_err(DatabaseExportError::from)
        );
        while let Some(items) = unwrap_else_send_error_and_return!(
            self,
            exporter.next_batch(Self::ITEM_BATCH_SIZE).map_err(DatabaseExportError::from)
        ) {
            let mut batch = Vec::with_capacity(items.len());
            for item in items {
                match encode_item(item) {
                    EncodedItem::Schema(schema) => {
                        unwrap_else_send_error_and_return!(self, self.send_schema(schema).await)
                    }
                    EncodedItem::Item(item) => batch.push(item),
                }
            }
            self.count_items(&batch);
            unwrap_else_send_error_and_return!(self, self.send_items(batch).await);
        }

        unwrap_else_send_error_and_return!(self, self.send_done().await);
        event!(
            Level::INFO,
            "Export '{}' from TypeDB {} finished successfully. {} items exported in {} seconds.",
            self.database_name,
            self.distribution_info.version,
            self.total_item_count,
            start.elapsed().as_secs()
        );
    }

    fn count_items(&mut self, batch: &[MigrationItemProto]) {
        let concepts = batch
            .iter()
            .filter(|item| {
                matches!(
                    item.item,
                    Some(MigrationItem::Entity(_))
                        | Some(MigrationItem::Relation(_))
                        | Some(MigrationItem::Attribute(_))
                )
            })
            .count() as u64;
        let previous_intervals = self.total_item_count / ITEMS_LOG_INTERVAL;
        self.total_item_count += concepts;
        if self.total_item_count / ITEMS_LOG_INTERVAL > previous_intervals {
            event!(Level::DEBUG, "Processed {} exported items of '{}'...", self.total_item_count, self.database_name);
        }
    }

    async fn send_message(&mut self, message: ProtocolServer) -> Result<(), DatabaseExportError> {
        tokio::select! { biased;
            _ = self.shutdown_receiver.changed() => Err(DatabaseExportError::ShutdownInterrupt {}),
            _ = self.close_receiver.recv() => Err(DatabaseExportError::TransactionCloseInterrupt {}),
            result = self.response_sender.send(Ok(message)) => {
                if let Err(err) = &result {
                    event!(Level::TRACE, "Send database export message failed: {:?}", err);
                }
                result.map_err(|_| DatabaseExportError::ClientChannelIsClosed {})
            }
        }
    }

    fn send_error(response_sender: &ResponseSender, error: DatabaseExportError) {
        Self::send_terminal_status(
            response_sender,
            LocalServerStateError::DatabaseExport { typedb_source: error }.into_status(),
        );
    }

    fn send_terminal_status(response_sender: &ResponseSender, status: Status) {
        match response_sender.try_send(Err(status)) {
            Ok(()) => (),
            Err(TrySendError::Full(message)) => {
                let sender = response_sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(message).await;
                });
            }
            Err(TrySendError::Closed(message)) => {
                event!(Level::TRACE, "Send database export terminal message failed: {:?}", message);
            }
        }
    }

    async fn send_schema(&mut self, schema: String) -> Result<(), DatabaseExportError> {
        self.send_message(database_export_initial_res_ok(schema)).await
    }

    async fn send_items(&mut self, items: Vec<MigrationItemProto>) -> Result<(), DatabaseExportError> {
        self.send_message(database_export_res_part_items(items)).await
    }

    async fn send_done(&mut self) -> Result<(), DatabaseExportError> {
        self.send_message(database_export_res_done()).await
    }

    async fn open_transaction(&self) -> Option<TransactionRead<WALClient>> {
        let opened = self
            .server_state
            .transactions()
            .open(
                &self.database_name,
                self.owner.clone(),
                TransactionType::Read,
                Self::transaction_options(),
                self.close_sender.clone(),
            )
            .await;
        match opened {
            Ok(Transaction::Read(transaction)) => Some(transaction),
            Ok(_) => {
                unreachable!("Expected a read transaction for an export")
            }
            Err(err) => {
                Self::send_terminal_status(&self.response_sender, err.into_status());
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
