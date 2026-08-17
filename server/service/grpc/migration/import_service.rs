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

use database::migration::database_importer::DatabaseImporter;
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use error::TypeDBError;
use executor::{ExecutionInterrupt, InterruptType};
use tokio::{
    sync::{
        broadcast,
        mpsc::{Receiver, Sender},
        watch,
    },
    task::{JoinHandle, spawn_blocking},
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
            diagnostics::run_with_diagnostics_async,
            error::{IntoGrpcStatus, ProtocolError},
            response_builders::database_manager::database_import_res_done,
        },
        import_service::DatabaseImportServiceError,
        migration::item_apply::process_item,
    },
    state::ServerState,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;
const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
struct ActiveImport {
    name: String,
    started: Instant,
    importer: Option<DatabaseImporter>,
}

impl ActiveImport {
    fn new(name: String, importer: DatabaseImporter) -> Self {
        Self { name, started: Instant::now(), importer: Some(importer) }
    }

    fn take_importer(&mut self) -> Option<DatabaseImporter> {
        self.importer.take()
    }

    fn restore_importer(&mut self, importer: DatabaseImporter) {
        self.importer = Some(importer);
    }

    fn elapsed_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }
}

#[derive(Debug)]
pub struct DatabaseImportService {
    server_state: Arc<ServerState>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,

    active: Option<ActiveImport>,
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
            active: None,
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

    async fn run_importer_step<T: Send + 'static>(
        &mut self,
        phase: &'static str,
        step: impl FnOnce(&mut DatabaseImporter) -> T + Send + 'static,
    ) -> Result<T, DatabaseImportServiceError> {
        let mut importer = self.take_importer(phase)?;
        let (importer, value) = self
            .run_step(
                spawn_blocking(move || {
                    let value = step(&mut importer);
                    (importer, value)
                }),
                phase,
            )
            .await?;
        self.active.as_mut().expect("Expected the import to be recorded").restore_importer(importer);
        Ok(value)
    }

    fn take_importer(&mut self, phase: &str) -> Result<DatabaseImporter, DatabaseImportServiceError> {
        self.active
            .as_mut()
            .and_then(ActiveImport::take_importer)
            .ok_or_else(|| DatabaseImportServiceError::ImportDatabaseNotFound { phase: phase.to_string() })
    }

    async fn run_step<T>(
        &mut self,
        mut step: JoinHandle<T>,
        phase: &'static str,
    ) -> Result<T, DatabaseImportServiceError> {
        let interrupted = tokio::select! { biased;
            _ = self.shutdown_receiver.changed() => DatabaseImportServiceError::ShutdownInterrupt {},
            _ = self.close_receiver.recv() => DatabaseImportServiceError::ImportClosed {},
            result = &mut step => return result.map_err(|_| Self::import_task_failed(phase)),
        };
        let _ = self.interrupt_sender.send(InterruptType::DatabaseImportAborted);
        let _ = step.await;
        Err(interrupted)
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
                        self.handle_database_schema(name, schema).await.map_err(|typedb_source| {
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

    async fn handle_database_schema(
        &mut self,
        name: String,
        schema: String,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        if let Some(active) = self.active.as_ref() {
            return Err(DatabaseImportServiceError::DuplicateImport { name, old_name: active.name.clone() });
        }

        let importer = self
            .server_state
            .databases()
            .import_prepare(
                &name,
                self.close_sender.clone(),
                ExecutionInterrupt::new(self.interrupt_sender.subscribe()),
            )
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::ImportPrepareFailed { typedb_source })?;
        self.active = Some(ActiveImport::new(name, importer));

        self.run_importer_step("schema loading", move |importer| importer.import_schema(schema))
            .await?
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;
        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        self.run_importer_step("data loading", move |importer| {
            for item in items {
                process_item(item, importer)?;

                let total_items = importer.total_item_count();
                if total_items != 0 && total_items % ITEMS_LOG_INTERVAL == 0 {
                    let name = importer.database_name();
                    event!(Level::DEBUG, "Processed {total_items} imported items of '{name}'...");
                }
            }
            Ok(())
        })
        .await??;
        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let importer = self.take_importer("finalisation")?;

        event!(Level::DEBUG, "Finalising the imported database...");
        let (total_items, result) = self
            .run_step(
                spawn_blocking(move || {
                    let total_items = importer.total_item_count();
                    (total_items, importer.import_done())
                }),
                "finalisation",
            )
            .await?;
        result.map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        let active = self.active.take().expect("Expected the import to be recorded");
        event!(
            Level::INFO,
            "Import to '{}' finished successfully. {total_items} items imported in {} seconds.",
            active.name,
            active.elapsed_secs(),
        );
        Self::send_done(&self.response_sender).await;
        Ok(Break(()))
    }

    async fn do_close(&mut self) {
        let Some(active) = self.active.take() else {
            return;
        };
        let duration_secs = active.elapsed_secs();
        let ActiveImport { name, importer, .. } = active;
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
