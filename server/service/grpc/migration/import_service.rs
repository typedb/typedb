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
use database::migration::database_importer::DatabaseImportError;
use database::migration::{item::MigrationMessage};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use error::TypeDBError;
use executor::{ExecutionInterrupt, InterruptType};
use std::{
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Instant,
};
use tokio::{
    sync::{
        broadcast,
        mpsc::{Receiver, Sender},
        watch,
    },
};
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    database_manager::import::{Client as ProtocolClient, Server as ProtocolServer},
    migration::Item as MigrationItemProto,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1000;
const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
struct ActiveImport {
    name: String,
    started: Instant,
    import_handle: JoinHandle<Result<u64, DatabaseImportError>>,
    item_sender: Sender<MigrationMessage>,
}

impl ActiveImport {
    fn new(
        name: String,
        import_handle: JoinHandle<Result<u64, DatabaseImportError>>,
        item_sender: Sender<MigrationMessage>,
    ) -> Self {
        Self { name, started: Instant::now(), import_handle, item_sender }
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
    interrupt_sender: broadcast::Sender<InterruptType>,
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
        let (interrupt_sender, _) = broadcast::channel(1);
        Self {
            server_state,
            diagnostics_manager,
            request_stream,
            response_sender,
            shutdown_receiver,
            active_import: None,
            interrupt_sender,
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

        let (item_sender, item_receiver) = tokio::sync::mpsc::channel(IMPORT_RESPONSE_BUFFER_SIZE);
        let importer = self
            .server_state
            .databases()
            .import_prepare(
                &name,
                self.close_sender.clone(),
                ExecutionInterrupt::new(self.interrupt_sender.subscribe()),
                item_receiver,
            )
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::ImportPrepareFailed { typedb_source })?;

        let import_task = spawn_blocking(|| importer.listen());

        item_sender
            .send(MigrationMessage::Schema(schema))
            .await
            .map_err(|_err| DatabaseImportServiceError::ChannelError {})?;

        self.active_import = Some(ActiveImport::new(name, import_task, item_sender));

        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let active_import = self.active_import.as_ref().expect("Import must be active");
        for item in items {
            active_import
                .item_sender
                .send(decode_item(item).map_err(DatabaseImportServiceError::from)?)
                .await
                .map_err(|_| DatabaseImportServiceError::ChannelError {})?;

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

        let ActiveImport { name, started, import_handle, item_sender } = active;

        event!(Level::DEBUG, "Finalising the imported database...");
        let _ = item_sender.send(MigrationMessage::Finalize).await
            .map_err(|_err| DatabaseImportServiceError::ChannelError {})?;

        let total_items = match import_handle.await
            .map_err(|_err| DatabaseImportServiceError::ThreadingError {})? {
            Ok(total_items) => total_items,
            Err(err) => {
                return Err(DatabaseImportServiceError::DatabaseImport { typedb_source: err });
            }
        };

        let elapsed = started.elapsed().as_secs();

        event!(
            Level::INFO,
            "Import to '{name}' finished successfully. {total_items} items imported in {elapsed} seconds.",
        );
        Self::send_done(&self.response_sender).await;
        Ok(Break(()))
    }

    // TODO: revisit
    async fn run_step<T>(
        &mut self,
        mut step: JoinHandle<T>,
        phase: &'static str,
    ) -> Result<T, DatabaseImportServiceError> {
        let interrupted = tokio::select! { biased;
            _ = self.shutdown_receiver.changed() => DatabaseImportServiceError::ShutdownInterrupt {},
            _ = self.close_receiver.recv() => DatabaseImportServiceError::ImportClosed {},
            _ = self.response_sender.closed() => DatabaseImportServiceError::ClientClosed {},
            result = &mut step => return result.map_err(|_| Self::import_task_failed(phase)),
        };
        let _ = self.interrupt_sender.send(InterruptType::DatabaseImportAborted);
        let _ = step.await;
        Err(interrupted)
    }

    async fn do_close(&mut self) {
        let Some(active) = self.active_import.take() else {
            return;
        };
        let ActiveImport { name, started, import_handle: importer, .. } = active;
        let duration_secs = started.elapsed().as_secs();
        drop(importer);
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

    fn import_task_failed(phase: &str) -> DatabaseImportServiceError {
        event!(Level::ERROR, "Import processing panicked during {phase}; the import will be cancelled.");
        DatabaseImportServiceError::ImportTaskFailed { phase: phase.to_string() }
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
