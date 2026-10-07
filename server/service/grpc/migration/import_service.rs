/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Instant,
};

use database::migration::{
    database_importer::{DatabaseImportError, ImporterHandle},
    item::MigrationItem,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use error::TypeDBError;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{Level, event};
use typedb_protocol::{
    database_manager::import::{Client as ProtocolClient, Server as ProtocolServer},
    migration::Item as MigrationItemProto,
};

use crate::{
    error::LocalServerStateError,
    service::{
        grpc::{
            diagnostics::submit_result_metrics,
            error::{IntoGrpcStatus, ProtocolError},
            response_builders::database_manager::database_import_res_done,
        },
        import_service::DatabaseImportServiceError,
        migration::item::decode_item,
    },
    state::ServerState,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;
const IMPORT_MESSAGE_BUFFER_SIZE: usize = 16;
// const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
struct ActiveImport {
    name: String,
    started: Instant,
    importer: ImporterHandle,
}

impl ActiveImport {
    fn new(name: String, importer: ImporterHandle) -> Self {
        Self { name, started: Instant::now(), importer }
    }
}

#[derive(Debug)]
struct StopSignals {
    shutdown_receiver: watch::Receiver<()>,
    close_receiver: Receiver<()>,
    response_sender: ResponseSender,
}

impl StopSignals {
    async fn stopped(&mut self) -> DatabaseImportServiceError {
        tokio::select! { biased;
            _ = self.shutdown_receiver.changed() => {
                event!(Level::TRACE, "Shutdown signal received, closing database import service.");
                DatabaseImportServiceError::ShutdownInterrupt {}
            }
            _ = self.close_receiver.recv() => {
                event!(Level::TRACE, "Close signal received, closing database import service.");
                DatabaseImportServiceError::ImportClosed {}
            }
            _ = self.response_sender.closed() => {
                event!(Level::TRACE, "Response stream closed by the client, closing database import service.");
                DatabaseImportServiceError::ClientClosed {}
            }
        }
    }

    async fn unless_stopped<T>(&mut self, future: impl Future<Output = T>) -> Result<T, DatabaseImportServiceError> {
        tokio::select! { biased;
            error = self.stopped() => Err(error),
            output = future => Ok(output),
        }
    }
}

#[derive(Debug)]
pub struct DatabaseImportService {
    server_state: Arc<ServerState>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    stop_signals: StopSignals,

    active_import: Option<ActiveImport>,
    database_name: Option<String>,
    close_sender: Sender<()>,
}

impl DatabaseImportService {
    pub(crate) fn new(
        server_state: Arc<ServerState>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        request_stream: Streaming<ProtocolClient>,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        let (close_sender, close_receiver) = tokio::sync::mpsc::channel(1);
        let stop_signals = StopSignals { shutdown_receiver, close_receiver, response_sender: response_sender.clone() };
        Self {
            server_state,
            diagnostics_manager,
            request_stream,
            response_sender,
            stop_signals,
            active_import: None,
            database_name: None,
            close_sender,
        }
    }

    pub(crate) async fn listen(mut self) {
        let result = self.listen_loop().await;
        if let Some(database_name) = &self.database_name {
            submit_result_metrics(&self.diagnostics_manager, Some(database_name), ActionKind::DatabasesImport, &result);
        }
    }

    async fn listen_loop(&mut self) -> Result<(), Status> {
        loop {
            // The importer's own result comes first, so that its error is reported rather than a stop that follows it.
            let result = tokio::select! { biased;
                import_result = Self::importer_stopped(&mut self.active_import) => {
                    self.handle_importer_stopped(import_result).await
                }
                error = self.stop_signals.stopped() => Err(Self::error_import_status(error)),
                next = self.request_stream.next() => self.handle_next(next).await,
            };

            match result {
                Ok(Continue(())) => (),
                Ok(Break(())) => {
                    event!(Level::TRACE, "Stream ended, closing database import service.");
                    return self.close().await;
                }
                Err(status) => {
                    event!(Level::TRACE, "Closing database import service after a failure.");
                    return self.close_with_error(status).await;
                }
            }
        }
    }

    /// Waits for the active import's importer to stop by itself. Never resolves without an active import.
    async fn importer_stopped(active_import: &mut Option<ActiveImport>) -> Result<u64, DatabaseImportError> {
        match active_import {
            Some(active_import) => active_import.importer.finish().await,
            None => std::future::pending().await,
        }
    }

    async fn handle_importer_stopped(
        &mut self,
        result: Result<u64, DatabaseImportError>,
    ) -> Result<ControlFlow<(), ()>, Status> {
        match result {
            Ok(total_items) => {
                self.notify_done(total_items).await;
                Ok(Break(()))
            }
            Err(typedb_source) => {
                Err(Self::error_import_status(DatabaseImportServiceError::DatabaseImport { typedb_source }))
            }
        }
    }

    async fn handle_next(
        &mut self,
        next: Option<Result<ProtocolClient, Status>>,
    ) -> Result<ControlFlow<(), ()>, Status> {
        match next {
            None => Ok(Break(())),
            Some(Err(error)) => {
                event!(Level::DEBUG, ?error, "GRPC error");
                Ok(Break(()))
            }
            Some(Ok(message)) => match message.client {
                None => Err(ProtocolError::MissingField {
                    name: "client",
                    description: "Database import message must contain a client request.",
                }
                .into_status()),
                Some(client) => match client.client {
                    None => Err(ProtocolError::MissingField {
                        name: "client",
                        description: "Database import message must contain a request.",
                    }
                    .into_status()),
                    Some(client) => self.handle_request(client).await,
                },
            },
        }
    }

    async fn handle_request(
        &mut self,
        req: typedb_protocol::migration::import::client::Client,
    ) -> Result<ControlFlow<(), ()>, Status> {
        use typedb_protocol::migration::import::client::{Client, Done, InitialReq, ReqPart};
        match req {
            Client::InitialReq(InitialReq { name, schema }) => self
                .handle_initialize(name, schema)
                .await
                .map_err(|typedb_source| LocalServerStateError::DatabaseImport { typedb_source }.into_status()),
            Client::ReqPart(ReqPart { items }) => self
                .handle_items(items)
                .await
                .map_err(|typedb_source| LocalServerStateError::DatabaseImport { typedb_source }.into_status()),
            Client::Done(Done {}) => self
                .handle_done()
                .await
                .map_err(|typedb_source| LocalServerStateError::DatabaseImport { typedb_source }.into_status()),
        }
    }

    async fn handle_initialize(
        &mut self,
        name: String,
        schema: String,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        if let Some(active) = self.active_import.as_ref() {
            return Err(DatabaseImportServiceError::DuplicateImport { name, old_name: active.name.clone() });
        }
        self.database_name = Some(name.clone());

        let importer = self
            .server_state
            .databases()
            .import_prepare(&name, self.close_sender.clone())
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::ImportPrepareFailed { typedb_source })?
            .start(IMPORT_MESSAGE_BUFFER_SIZE);

        // Recorded before anything can fail, so that closing the service cleans the import up.
        let active_import = self.active_import.insert(ActiveImport::new(name, importer));
        self.stop_signals
            .unless_stopped(active_import.importer.send(MigrationItem::Schema(schema)))
            .await?
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let active_import = self.active_import.as_mut().ok_or(DatabaseImportServiceError::ImportDatabaseNotFound {})?;
        self.stop_signals
            .unless_stopped(active_import.importer.send_batch(items.into_iter().map(decode_item)))
            .await?
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        // TODO: submitted data isn't really a good indicator but is at least a fixed lag. Maybe this actually reads a real value?
        // let total_items = active_import.importer.total_item_count();
        // if total_items != 0 && total_items % ITEMS_LOG_INTERVAL == 0 {
        //     let name = &active_import.name;
        //     event!(Level::DEBUG, "Submitted {total_items} imported items of '{name}'...");
        // }
        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        // The import stays active until it is finalised, so that a failure or a stop still cleans it up.
        let active_import = self.active_import.as_mut().ok_or(DatabaseImportServiceError::ImportDatabaseNotFound {})?;

        event!(Level::DEBUG, "Finalising the imported database...");
        let total_items = self
            .stop_signals
            .unless_stopped(active_import.importer.finalize())
            .await?
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        self.notify_done(total_items).await;
        Ok(Break(()))
    }

    async fn notify_done(&mut self, total_items: u64) {
        if let Some(ActiveImport { name, started, .. }) = self.active_import.take() {
            Self::log_imported(&name, started, total_items);
        }
        Self::send_done(&self.response_sender).await;
    }

    async fn close(&mut self) -> Result<(), Status> {
        let abandoned = self.active_import.is_some();
        if !abandoned || self.do_close().await {
            return Ok(());
        }
        Err(Self::error_import_status(DatabaseImportServiceError::ClientClosed {}))
    }

    async fn close_with_error(&mut self, status: Status) -> Result<(), Status> {
        if self.do_close().await {
            Self::send_done(&self.response_sender).await;
            return Ok(());
        }
        Self::send_error(&self.response_sender, status.clone()).await;
        Err(status)
    }

    async fn do_close(&mut self) -> bool {
        let Some(ActiveImport { name, started, importer }) = self.active_import.take() else {
            return false;
        };
        if let Ok(total_items) = importer.abort().await {
            Self::log_imported(&name, started, total_items);
            return true;
        }
        let elapsed = started.elapsed().as_secs();
        event!(Level::INFO, "Import to '{name}' finished without completion after {elapsed} seconds.");
        if let Err(err) = self.server_state.databases().import_discard(&name).await {
            event!(
                Level::ERROR,
                "Failed to clean up unfinished import of '{name}': {}",
                err.format_code_and_description()
            );
        }
        false
    }

    fn log_imported(name: &str, started: Instant, total_items: u64) {
        let elapsed = started.elapsed().as_secs();
        event!(
            Level::INFO,
            "Import to '{name}' finished successfully. {total_items} items imported in {elapsed} seconds."
        );
    }

    fn error_import_status(error: DatabaseImportServiceError) -> Status {
        LocalServerStateError::DatabaseImport { typedb_source: error }.into_status()
    }

    async fn send_done(response_sender: &ResponseSender) {
        if let Err(err) = response_sender.send(Ok(database_import_res_done())).await {
            event!(Level::DEBUG, "Submit database import done message failed: {:?}", err);
        }
    }

    async fn send_error(response_sender: &ResponseSender, status: Status) {
        if let Err(err) = response_sender.send(Err(status)).await {
            event!(Level::DEBUG, "Submit database import error message failed: {:?}", err);
        }
    }
}
