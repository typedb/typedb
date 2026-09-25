/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::{
    error::LocalServerStateError,
    service::{
        grpc::{
            diagnostics::run_with_diagnostics_async,
            error::{IntoGrpcStatus, ProtocolError},
            response_builders::database_manager::database_import_res_done,
        },
        import_service::DatabaseImportServiceError,
        migration::item::decode_item,
    },
    state::ServerState,
};
use database::migration::{database_importer::ImporterHandle, item::MigrationItem};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use error::TypeDBError;
use std::{
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Instant,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    database_manager::import::{Client as ProtocolClient, Server as ProtocolServer},
    migration::Item as MigrationItemProto,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;
const IMPORT_MESSAGE_BUFFER_SIZE: usize = 1000;
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
pub struct DatabaseImportService {
    server_state: Arc<ServerState>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,

    active_import: Option<ActiveImport>,
    close_sender: Sender<()>,
    close_receiver: Receiver<()>,
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
        Self {
            server_state,
            diagnostics_manager,
            request_stream,
            response_sender,
            shutdown_receiver,
            active_import: None,
            close_sender,
            close_receiver,
        }
    }

    pub(crate) async fn listen(mut self) {
        loop {
            let result = tokio::select! { biased;
                _ = self.shutdown_receiver.changed() => {
                    event!(Level::TRACE, "Shutdown signal received, closing database import service.");
                    self.close_with_error(Self::import_status(DatabaseImportServiceError::ShutdownInterrupt {})).await;
                    return;
                }
                _ = self.close_receiver.recv() => {
                    event!(Level::TRACE, "Close signal received, closing database import service.");
                    self.close_with_error(Self::import_status(DatabaseImportServiceError::ImportClosed {})).await;
                    return;
                }
                next = self.request_stream.next() => {
                    self.handle_next(next).await
                }
            };

            match result {
                Ok(Continue(())) => (),
                Ok(Break(())) => {
                    event!(Level::TRACE, "Stream ended, closing database import service.");
                    self.do_close().await;
                    return;
                }
                Err(status) => {
                    event!(Level::TRACE, "Closing database import service after a failure.");
                    self.close_with_error(status).await;
                    return;
                }
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
            Client::InitialReq(InitialReq { name, schema }) => {
                run_with_diagnostics_async(
                    self.diagnostics_manager.clone(),
                    Some(name.clone()),
                    ActionKind::DatabasesImport,
                    || async {
                        self.handle_initialize(name, schema).await.map_err(|typedb_source| {
                            LocalServerStateError::DatabaseImport { typedb_source }.into_status()
                        })
                    },
                )
                .await
            }
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

        let mut importer = self
            .server_state
            .databases()
            .import_prepare(&name, self.close_sender.clone())
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::ImportPrepareFailed { typedb_source })?
            .start(IMPORT_MESSAGE_BUFFER_SIZE);

        importer
            .send(MigrationItem::Schema(schema))
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        self.active_import = Some(ActiveImport::new(name, importer));

        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let active_import =
            self.active_import.as_mut().ok_or(DatabaseImportServiceError::ImportDatabaseNotFound {})?;
        for item in items {
            active_import
                .importer
                .send(decode_item(item).map_err(DatabaseImportServiceError::from)?)
                .await
                .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

            // TODO: submitted data isn't really a good indicator but is at least a fixed lag. Maybe this actually reads a real value?
            // let total_items = active_import.importer.total_item_count();
            // if total_items != 0 && total_items % ITEMS_LOG_INTERVAL == 0 {
            //     let name = &active_import.name;
            //     event!(Level::DEBUG, "Submitted {total_items} imported items of '{name}'...");
            // }
        }
        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let active: ActiveImport = match self.active_import.take() {
            None => return Err(DatabaseImportServiceError::ImportDatabaseNotFound {}),
            Some(active) => active,
        };

        let ActiveImport { name, started, importer } = active;

        event!(Level::DEBUG, "Finalising the imported database...");
        let total_items = importer
            .finalize()
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        let elapsed = started.elapsed().as_secs();

        event!(
            Level::INFO,
            "Import to '{name}' finished successfully. {total_items} items imported in {elapsed} seconds.",
        );
        Self::send_done(&self.response_sender).await;
        Ok(Break(()))
    }

    async fn do_close(&mut self) {
        let Some(active) = self.active_import.take() else {
            return;
        };
        let ActiveImport { name, started, importer } = active;
        let duration_secs = started.elapsed().as_secs();
        // Wait for the importer to stop before discarding the database it writes to.
        let _ = importer.abort().await;
        event!(Level::INFO, "Import to '{name}' finished without completion after {duration_secs} seconds.");
        if let Err(err) = self.server_state.databases().import_discard(&name).await {
            event!(
                Level::ERROR,
                "Failed to clean up unfinished import of '{name}': {}",
                err.format_code_and_description()
            );
        }
    }

    async fn close_with_error(&mut self, status: Status) {
        self.do_close().await;
        Self::send_error(&self.response_sender, status).await;
    }

    fn import_status(error: DatabaseImportServiceError) -> Status {
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
