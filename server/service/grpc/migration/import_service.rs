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

use database::{database_manager::DatabaseManager, migration::database_importer::DatabaseImporter};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use encoding::value::label::Label;
use itertools::Itertools;
use tokio::sync::{mpsc::Sender, watch};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    database_manager::import::{Client as ProtocolClient, Server as ProtocolServer},
    migration::{
        item::{
            relation::{role::Player as MigrationRolePlayerProto, Role as MigrationRoleProto},
            Attribute as MigrationAttributeProto, Checksums as MigrationChecksumsProto, Entity as MigrationEntityProto,
            Header as MigrationHeaderProto, OwnedAttribute as MigrationOwnedAttributeProto,
            Relation as MigrationRelationProto,
        },
        Item as MigrationItemProto,
    },
};

use crate::service::{
    grpc::{
        diagnostics::run_with_diagnostics_async,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
        migration::item::{decode_checksums, decode_migration_value},
        response_builders::database_manager::database_import_res_done,
    },
    import_service::DatabaseImportServiceError,
};

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;
const ITEMS_LOG_INTERVAL: u64 = 1_000_000;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
pub(crate) struct DatabaseImportService {
    database_manager: Arc<DatabaseManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,

    database_importer: Option<DatabaseImporter>,
    is_done: bool,
    start: Option<Instant>,
}

impl DatabaseImportService {
    pub(crate) fn new(
        database_manager: Arc<DatabaseManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        request_stream: Streaming<ProtocolClient>,
        response_sender: ResponseSender,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        Self {
            database_manager,
            diagnostics_manager,
            request_stream,
            response_sender,
            shutdown_receiver,
            database_importer: None,
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
                None => {
                    return Err(ProtocolError::MissingField {
                        name: "client",
                        description: "Database import message must contain a client request.",
                    }
                    .into_status());
                }
                Some(client) => match client.client {
                    None => {
                        return Err(ProtocolError::MissingField {
                            name: "client",
                            description: "Database import message must contain a request.",
                        }
                        .into_status());
                    }
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
                        self.handle_database_schema(name, schema)
                            .await
                            .map_err(|typedb_source| typedb_source.into_error_message().into_status())
                    },
                )
                .await
            }
            Client::ReqPart(ReqPart { items }) => {
                self.handle_items(items).await.map_err(|typedb_source| typedb_source.into_error_message().into_status())
            }
            Client::Done(Done {}) => {
                self.handle_done().await.map_err(|typedb_source| typedb_source.into_error_message().into_status())
            }
        }
    }

    async fn handle_database_schema(
        &mut self,
        name: String,
        schema: String,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        self.start = Some(Instant::now());
        if let Some(database_importer) = self.database_importer.as_ref() {
            return Err(DatabaseImportServiceError::DuplicateImport {
                name,
                old_name: database_importer.database_name().to_string(),
            });
        }

        let database_importer = DatabaseImporter::new(self.database_manager.clone(), name)
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;
        self.database_importer = Some(database_importer);

        self.database_importer
            .as_mut()
            .unwrap()
            .import_schema(schema)
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;
        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let database_importer = match self.database_importer.as_mut() {
            Some(database_importer) => database_importer,
            None => return Err(DatabaseImportServiceError::DatabaseNotFoundForItems {}),
        };

        for item in items {
            Self::process_item(item, database_importer).await?;

            let total_items = database_importer.total_item_count();
            if total_items != 0 && total_items % ITEMS_LOG_INTERVAL == 0 {
                let name = database_importer.database_name();
                event!(Level::DEBUG, "Processed {total_items} imported items of '{name}'...");
            }
        }

        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportServiceError> {
        let database_importer = match &mut self.database_importer {
            Some(database_importer) => database_importer,
            None => return Err(DatabaseImportServiceError::DatabaseNotFoundForDone {}),
        };

        event!(Level::DEBUG, "Finalising the imported database...");
        database_importer
            .import_done()
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })?;

        Self::send_done(&self.response_sender).await;
        self.is_done = true;
        Ok(Break(()))
    }

    async fn do_close(&mut self) {
        let Some(database_importer) = self.database_importer.take() else {
            return;
        };

        let database_name = database_importer.database_name();
        let duration_secs = self.start.unwrap_or(Instant::now()).elapsed().as_secs();
        if self.is_done {
            event!(
                Level::INFO,
                "Import to '{}' finished successfully. {} items imported in {} seconds.",
                database_name,
                database_importer.total_item_count(),
                duration_secs,
            );
        } else {
            event!(
                Level::INFO,
                "Import to '{database_name}' finished without completion after {duration_secs} seconds.",
            );
        }
        // Cleanup the import resources before proceeding
        drop(database_importer)
    }

    async fn process_item(
        item_proto: MigrationItemProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        use typedb_protocol::migration::item;
        let MigrationItemProto { item } = item_proto;
        let Some(item) = item else {
            return Err(DatabaseImportServiceError::EmptyItem {});
        };

        match item {
            item::Item::Attribute(attribute) => Self::process_attribute(attribute, database_importer).await,
            item::Item::Entity(entity) => Self::process_entity(entity, database_importer).await,
            item::Item::Relation(relation) => Self::process_relation(relation, database_importer).await,
            item::Item::Header(header) => Self::process_header(database_importer, header),
            item::Item::Checksums(checksums) => Self::process_checksums(database_importer, checksums),
        }
    }

    async fn process_attribute(
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
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    async fn process_entity(
        entity_proto: MigrationEntityProto,
        database_importer: &mut DatabaseImporter,
    ) -> Result<(), DatabaseImportServiceError> {
        let MigrationEntityProto { id, label: label_text, attributes } = entity_proto;
        let label = Label::parse_from(&label_text, None);

        database_importer
            .import_entity(id, label, Self::convert_owned_attributes(attributes))
            .await
            .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
    }

    async fn process_relation(
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
            .await
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
