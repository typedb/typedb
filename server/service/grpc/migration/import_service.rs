/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    fmt,
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Instant,
};

use database::{
    migration::database_importer::{DatabaseImporter, ImportCommitError, ImportCommitter},
    transaction::{DataCommitIntent, SchemaCommitIntent},
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use encoding::value::label::Label;
use error::TypeDBError;
use itertools::Itertools;
use storage::durability_client::WALClient;
use tokio::{
    runtime::Handle,
    sync::{mpsc::Sender, watch},
    task::spawn_blocking,
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{Level, event};
use typedb_protocol::{
    database_manager::import::{Client as ProtocolClient, Server as ProtocolServer},
    migration::{
        Item as MigrationItemProto,
        item::{
            Attribute as MigrationAttributeProto, Checksums as MigrationChecksumsProto, Entity as MigrationEntityProto,
            Header as MigrationHeaderProto, OwnedAttribute as MigrationOwnedAttributeProto,
            Relation as MigrationRelationProto,
            relation::{Role as MigrationRoleProto, role::Player as MigrationRolePlayerProto},
        },
    },
};

use crate::{
    error::LocalServerStateError,
    service::{
        grpc::{
            diagnostics::run_with_diagnostics_async,
            error::{IntoGrpcStatus, ProtocolError},
            migration::item::{decode_checksums, decode_migration_value},
            response_builders::database_manager::database_import_res_done,
        },
        import_service::DatabaseImportServiceError,
    },
    state::ServerState,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;
const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

struct ImportServiceCommitter {
    server_state: Arc<ServerState>,
    runtime: Handle,
}

impl ImportCommitter for ImportServiceCommitter {
    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), ImportCommitError> {
        self.runtime.block_on(self.server_state.databases().import_schema_commit(intent)).map_err(|err| err as _)
    }

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), ImportCommitError> {
        self.runtime.block_on(self.server_state.databases().import_data_commit(intent)).map_err(|err| err as _)
    }
}

struct ImportSession {
    name: String,
    server_state: Arc<ServerState>,
    importer: Option<DatabaseImporter>,
    completed: bool,
}

impl ImportSession {
    async fn open(name: String, server_state: Arc<ServerState>) -> Result<Self, DatabaseImportServiceError> {
        let database = server_state
            .databases()
            .import_prepare(&name)
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::ServerState { typedb_source })?;
        let committer =
            Box::new(ImportServiceCommitter { server_state: server_state.clone(), runtime: Handle::current() });
        let importer = DatabaseImporter::new(database, committer);
        Ok(Self { name, server_state, importer: Some(importer), completed: false })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn importer(&mut self) -> &mut DatabaseImporter {
        self.importer.as_mut().expect("Expected a live importer in an incomplete import session")
    }

    fn total_item_count(&self) -> u64 {
        self.importer.as_ref().map_or(0, |importer| importer.total_item_count())
    }

    fn release_importer(&mut self) {
        self.importer = None;
    }

    fn complete(mut self) {
        self.completed = true;
    }
}

impl fmt::Debug for ImportSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportSession").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Drop for ImportSession {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.importer = None;
        let name = std::mem::take(&mut self.name);
        let server_state = self.server_state.clone();
        match Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(err) = server_state.databases().import_cancel(&name).await {
                        event!(
                            Level::WARN,
                            "Failed to clean up abandoned import of '{name}': {}",
                            err.format_code_and_description()
                        );
                    }
                });
            }
            Err(_) => {
                event!(Level::ERROR, "No runtime to clean up abandoned import of '{name}'.");
            }
        }
    }
}

#[derive(Debug)]
pub struct DatabaseImportService {
    server_state: Arc<ServerState>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,

    session: Option<ImportSession>,
    is_done: bool,
    start: Option<Instant>,
}

impl DatabaseImportService {
    pub(crate) fn new(
        server_state: Arc<ServerState>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        request_stream: Streaming<ProtocolClient>,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        Self {
            server_state,
            diagnostics_manager,
            request_stream,
            response_sender,
            shutdown_receiver,
            session: None,
            is_done: false,
            start: None,
        }
    }

    pub(crate) async fn listen(mut self) {
        loop {
            let result = tokio::select! { biased;
                _ = self.shutdown_receiver.changed() => {
                    event!(Level::TRACE, "Shutdown signal received, closing database import service.");
                    self.do_close().await;
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
                    event!(Level::TRACE, "Stream ended with error, closing database import service.");
                    self.do_close().await; // Make sure to clean up before replying with an error
                    Self::send_error(&self.response_sender, status).await;
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
        self.start = Some(Instant::now());
        if let Some(session) = self.session.as_ref() {
            return Err(DatabaseImportServiceError::DuplicateImport { name, old_name: session.name().to_string() });
        }

        let session = ImportSession::open(name, self.server_state.clone()).await?;

        // The importer is synchronous: run it on a blocking thread, where its commits may legally
        // block on the async operator.
        let (session, result) = spawn_blocking(move || {
            let mut session = session;
            let result = session.importer().import_schema(schema);
            (session, result)
        })
        .await
        .expect("Import schema task panicked");
        self.session = Some(session);
        result.map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;
        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let Some(session) = self.session.take() else {
            return Err(DatabaseImportServiceError::ImportDatabaseNotFound { phase: "data loading".to_string() });
        };

        let (session, result) = spawn_blocking(move || {
            let mut session = session;
            let result = (|| {
                for item in items {
                    Self::process_item(item, session.importer())?;

                    let total_items = session.importer().total_item_count();
                    if total_items != 0 && total_items % ITEMS_LOG_INTERVAL == 0 {
                        let name = session.name();
                        event!(Level::DEBUG, "Processed {total_items} imported items of '{name}'...");
                    }
                }
                Ok(())
            })();
            (session, result)
        })
        .await
        .expect("Import items task panicked");
        self.session = Some(session);
        result?;
        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let Some(session) = self.session.take() else {
            return Err(DatabaseImportServiceError::ImportDatabaseNotFound { phase: "finalisation".to_string() });
        };

        event!(Level::DEBUG, "Finalising the imported database...");
        let (mut session, result) = spawn_blocking(move || {
            let mut session = session;
            let result = session.importer().import_done();
            (session, result)
        })
        .await
        .expect("Import done task panicked");
        if let Err(typedb_source) = result {
            self.session = Some(session);
            return Err(DatabaseImportServiceError::DatabaseImport { typedb_source });
        }

        let total_items = session.total_item_count();
        session.release_importer();
        match self.server_state.databases().import_finalise(session.name()).await {
            Ok(()) => {
                let duration_secs = self.start.unwrap_or(Instant::now()).elapsed().as_secs();
                event!(
                    Level::INFO,
                    "Import to '{}' finished successfully. {total_items} items imported in {duration_secs} seconds.",
                    session.name(),
                );
                session.complete();
            }
            Err(typedb_source) => {
                // Reinstate the session: `do_close` then cancels it before the error response is
                // sent, keeping cleanup synchronous (the session's spawned cancel is for panics only).
                self.session = Some(session);
                return Err(DatabaseImportServiceError::ServerState { typedb_source });
            }
        }

        Self::send_done(&self.response_sender).await;
        self.is_done = true;
        Ok(Break(()))
    }

    async fn do_close(&mut self) {
        if let Some(mut session) = self.session.take() {
            debug_assert!(!self.is_done, "Expected no import session after a successful import");
            let duration_secs = self.start.unwrap_or(Instant::now()).elapsed().as_secs();
            event!(
                Level::INFO,
                "Import to '{}' finished without completion after {duration_secs} seconds.",
                session.name(),
            );
            session.release_importer();
            if let Err(err) = self.server_state.databases().import_cancel(session.name()).await {
                event!(
                    Level::ERROR,
                    "Failed to clean up unfinished import of '{}': {}",
                    session.name(),
                    err.format_code_and_description()
                );
            }
            session.complete();
        }
    }

    fn process_item(
        item_proto: MigrationItemProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        use typedb_protocol::migration::item;
        let MigrationItemProto { item } = item_proto;
        let Some(item) = item else {
            return Err(DatabaseImportServiceError::ImportEmptyItem {});
        };

        match item {
            item::Item::Attribute(attribute) => Self::process_attribute(attribute, database_importer),
            item::Item::Entity(entity) => Self::process_entity(entity, database_importer),
            item::Item::Relation(relation) => Self::process_relation(relation, database_importer),
            item::Item::Header(header) => Self::process_header(database_importer, header),
            item::Item::Checksums(checksums) => Self::process_checksums(database_importer, checksums),
        }
    }

    fn process_attribute(
        attribute_proto: MigrationAttributeProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        let MigrationAttributeProto { id, label: label_text, attributes, value } = attribute_proto;
        if !attributes.is_empty() {
            return Err(DatabaseImportServiceError::AttributesOwningAttributes {});
        }
        let label = Label::parse_from(&label_text, None);
        let value = decode_migration_value(value.ok_or_else(|| DatabaseImportServiceError::AbsentAttributeValue {})?)
            .map_err(|typedb_source| DatabaseImportServiceError::ConceptDecode { typedb_source })?;

        database_importer
            .import_attribute(id, label, value)
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    fn process_entity(
        entity_proto: MigrationEntityProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        let MigrationEntityProto { id, label: label_text, attributes } = entity_proto;
        let label = Label::parse_from(&label_text, None);

        database_importer
            .import_entity(id, label, Self::convert_owned_attributes(attributes))
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    fn process_relation(
        relation_proto: MigrationRelationProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        let MigrationRelationProto { id, label: label_text, attributes, roles } = relation_proto;
        let label = Label::parse_from(&label_text, None);

        database_importer
            .import_relation(
                id,
                label,
                Self::convert_owned_attributes(attributes),
                Self::convert_related_role_players(roles),
            )
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    fn process_header(
        database_importer: &DatabaseImporter,
        header_proto: MigrationHeaderProto,
    ) -> Result<(), DatabaseImportServiceError> {
        let MigrationHeaderProto { typedb_version: original_version, original_database } = header_proto;
        let new_database = database_importer.database_name();
        event!(Level::DEBUG, "Importing '{original_database}' from TypeDB {original_version} to '{new_database}'.");
        Ok(())
    }

    fn process_checksums(
        database_importer: &mut DatabaseImporter,
        checksums_proto: MigrationChecksumsProto,
    ) -> Result<(), DatabaseImportServiceError> {
        database_importer
            .record_expected_checksums(decode_checksums(checksums_proto))
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    fn convert_owned_attributes(attributes: Vec<MigrationOwnedAttributeProto>) -> Vec<String> {
        attributes
            .into_iter()
            .map(|proto| {
                let MigrationOwnedAttributeProto { id } = proto;
                id
            })
            .collect_vec()
    }

    fn convert_related_role_players(roles: Vec<MigrationRoleProto>) -> Vec<(Label, Vec<String>)> {
        roles
            .into_iter()
            .map(|role_proto| {
                let MigrationRoleProto { label: label_text, players } = role_proto;
                let label = Label::parse_from(&label_text, None);
                (
                    label,
                    players
                        .into_iter()
                        .map(|proto| {
                            let MigrationRolePlayerProto { id } = proto;
                            id
                        })
                        .collect_vec(),
                )
            })
            .collect_vec()
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
