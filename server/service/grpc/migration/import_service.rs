/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use concept::{
    thing::{
        attribute::Attribute,
        entity::Entity,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::ThingManager,
        ThingAPI,
    },
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationCategory, AnnotationIndependent, AnnotationKey},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::Constraint,
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, Ordering, OwnerAPI, PlayerAPI,
    },
};
use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionSchema, TransactionWrite},
    Database,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use encoding::value::label::Label;
use options::TransactionOptions;
use regex::Regex;
use resource::{
    constants::{
        common::{SECONDS_IN_DAY, SECONDS_IN_HOUR},
        snapshot::BUFFER_KEY_INLINE,
    },
    profile::StorageCounters,
};
use storage::{
    durability_client::{DurabilityClient, WALClient},
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
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
use typeql::{parse_query, query::SchemaQuery};

use crate::service::{
    grpc::{
        diagnostics::run_with_diagnostics_async,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
        migration::{
            item::{decode_checksums, decode_migration_value},
            Checksums,
        },
        response_builders::database_manager::database_import_res_done,
    },
    import_service::DatabaseImportError,
    transaction_service::{execute_schema_query, with_transaction_parts},
};

macro_rules! is_specializing_with_only_cardinality_specializations_fn {
    (
        $fn_name:ident,
        $capability_ty:ty,
        $get_cardinality_method:ident
    ) => {
        fn $fn_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            capability: $capability_ty,
        ) -> Result<bool, DatabaseImportError> {
            let object_type = capability.object();
            let interface_type = capability.interface();
            let cardinalities = object_type
                .$get_cardinality_method(snapshot, type_manager, interface_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            let same_interface_type_count = cardinalities
                .into_iter()
                .filter(|constraint| constraint.source().interface() == interface_type)
                .count();

            // If this capability is affected by multiple cardinality constraints from the same interface type, then
            // the object type has multiple ownerships of this interface type: some inherited and one declared (specializing)
            if same_interface_type_count > 1 {
                let non_cardinality_count = capability
                    .get_annotations_declared(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                    .into_iter()
                    .map(|annotation| Into::<Annotation>::into(annotation.clone()).category())
                    .filter(|category| !matches!(category, &AnnotationCategory::Cardinality))
                    .count();
                Ok(non_cardinality_count == 0)
            } else {
                Ok(false)
            }
        }
    };
}

pub(crate) const IMPORT_RESPONSE_BUFFER_SIZE: usize = 1;

type ResponseSender = Sender<Result<ProtocolServer, Status>>;

#[derive(Debug)]
struct DatabaseInfo {
    database: Arc<Database<WALClient>>,
    schema_info: SchemaInfo,
    data_info: DataInfo,

    data_transaction: Option<TransactionWrite<WALClient>>,
    transaction_item_count: u64,
}

impl DatabaseInfo {
    fn new(database: Arc<Database<WALClient>>) -> Self {
        Self {
            database,
            schema_info: SchemaInfo::new(),
            data_info: DataInfo::new(),

            data_transaction: None,
            transaction_item_count: 0,
        }
    }

    fn database(&self) -> Arc<Database<WALClient>> {
        self.database.clone()
    }

    fn get_data_transaction(
        &mut self,
        database: Arc<Database<WALClient>>,
    ) -> Result<TransactionWrite<WALClient>, DatabaseImportError> {
        match self.data_transaction.take() {
            Some(transaction) => Ok(transaction),
            None => DatabaseImportService::open_write_transaction(database),
        }
    }
}

#[derive(Debug)]
struct SchemaInfo {
    temporarily_independent_attribute_types: HashSet<AttributeType>,
    temporarily_independent_relation_types: HashSet<RelationType>,
    original_keys: HashSet<Owns>,
    original_cardinalities_owns: HashMap<Owns, Option<AnnotationCardinality>>,
    original_cardinalities_plays: HashMap<Plays, Option<AnnotationCardinality>>,
    original_cardinalities_relates: HashMap<Relates, Option<AnnotationCardinality>>,
    temporarily_removed_specializing_owns: HashMap<Owns, (HashSet<OwnsAnnotation>, Ordering)>,
    temporarily_removed_specializing_plays: HashMap<Plays, HashSet<PlaysAnnotation>>,
}

impl SchemaInfo {
    fn new() -> Self {
        Self {
            temporarily_independent_attribute_types: HashSet::new(),
            temporarily_independent_relation_types: HashSet::new(),
            original_keys: HashSet::new(),
            original_cardinalities_owns: HashMap::new(),
            original_cardinalities_plays: HashMap::new(),
            original_cardinalities_relates: HashMap::new(),
            temporarily_removed_specializing_owns: HashMap::new(),
            temporarily_removed_specializing_plays: HashMap::new(),
        }
    }
}

type IID = Bytes<'static, BUFFER_KEY_INLINE>;

#[derive(Debug)]
struct DataInfo {
    objects: ObjectsInfo,
    attributes: AttributesInfo,
    checksums: Checksums,
    expected_checksums: Option<Checksums>,
}

impl DataInfo {
    fn new() -> Self {
        Self {
            objects: ObjectsInfo::new(),
            attributes: AttributesInfo::new(),
            checksums: Checksums::new(),
            expected_checksums: None,
        }
    }

    fn record_entity(&mut self, original_id: String, entity: Entity) {
        self.objects.instance_id_mapping.record(original_id, entity.into_object());
        self.checksums.entity_count += 1;
    }

    fn record_relation(&mut self, original_id: String, relation: Relation) {
        self.objects.instance_id_mapping.record(original_id, relation.into_object());
        self.checksums.relation_count += 1;
    }

    fn record_attribute(&mut self, original_id: String, attribute: Attribute) {
        self.attributes.instance_id_mapping.record(original_id, attribute);
        self.checksums.attribute_count += 1;
    }

    fn record_ownership(&mut self) {
        self.checksums.ownership_count += 1;
    }

    fn record_role(&mut self) {
        self.checksums.role_count += 1;
    }

    fn record_client_checksums(&mut self, checksums_proto: MigrationChecksumsProto) -> Result<(), DatabaseImportError> {
        if self.expected_checksums.is_some() {
            return Err(DatabaseImportError::DuplicateClientChecksums {});
        }
        self.expected_checksums = Some(decode_checksums(checksums_proto));
        Ok(())
    }

    fn verify_checksums(&self) -> Result<(), DatabaseImportError> {
        let self_checksums = &self.checksums;
        match &self.expected_checksums {
            Some(client_checksums) => {
                if self_checksums == client_checksums {
                    Ok(())
                } else {
                    let details = format!("client expected {client_checksums}, but recorded {self_checksums}");
                    Err(DatabaseImportError::InvalidChecksumsOnDone { details })
                }
            }
            None => Err(DatabaseImportError::NoClientChecksumsOnDone {}),
        }
    }
}

#[derive(Debug)]
struct InstanceIDMapping<T: ThingAPI> {
    original_to_buffered_ids: HashMap<String, IID>,
    _phantom_data: PhantomData<T>,
}

impl<T: ThingAPI> InstanceIDMapping<T> {
    fn new() -> Self {
        Self { original_to_buffered_ids: HashMap::new(), _phantom_data: PhantomData }
    }

    fn get_by_original_id(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        id: &str,
    ) -> Result<Option<T>, DatabaseImportError> {
        let Some(buffered_id) = self.original_to_buffered_ids.get(id) else {
            return Ok(None);
        };
        thing_manager
            .get_instance::<T>(snapshot, buffered_id, StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })
    }

    fn record(&mut self, original_id: String, instance: T) {
        self.original_to_buffered_ids.insert(original_id, instance.iid().into_owned());
    }
}

#[derive(Debug)]
struct ObjectsInfo {
    pub instance_id_mapping: InstanceIDMapping<Object>,
    pub awaited_for_roles: HashMap<String, HashSet<(RoleType, Relation)>>,
}

impl ObjectsInfo {
    fn new() -> Self {
        Self { instance_id_mapping: InstanceIDMapping::new(), awaited_for_roles: HashMap::new() }
    }
}

#[derive(Debug)]
struct AttributesInfo {
    pub instance_id_mapping: InstanceIDMapping<Attribute>,
    pub awaited_for_ownerships: HashMap<String, HashSet<Object>>,
}

impl AttributesInfo {
    fn new() -> Self {
        Self { instance_id_mapping: InstanceIDMapping::new(), awaited_for_ownerships: HashMap::new() }
    }
}

#[derive(Debug)]
pub(crate) struct DatabaseImportService {
    database_manager: Arc<DatabaseManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    request_stream: Streaming<ProtocolClient>,
    response_sender: ResponseSender,
    shutdown_receiver: watch::Receiver<()>,

    database_info: Option<DatabaseInfo>,
    is_done: bool,
}

impl DatabaseImportService {
    const OPTIONS_PARALLEL: bool = true;
    const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(1 * SECONDS_IN_DAY).as_millis() as u64;

    const COMMIT_BATCH_SIZE: u64 = 10_000;

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
            database_info: None,
            is_done: false,
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
    ) -> Result<ControlFlow<(), ()>, DatabaseImportError> {
        let database = match self.get_database(&name) {
            None => {
                self.create_database(&name)?;
                self.get_database(&name).ok_or_else(|| DatabaseImportError::CreatedDatabaseNotFound { name })?
            }
            Some(_) => return Err(DatabaseImportError::DatabaseAlreadyExists { name }),
        };

        self.database_info = Some(DatabaseInfo::new(database.clone()));

        Self::import_schema(database.clone(), schema).await?;
        Self::relax_schema(database.clone(), &mut self.database_info.as_mut().unwrap().schema_info)?;
        Ok(Continue(()))
    }

    async fn handle_items(
        &mut self,
        items: Vec<MigrationItemProto>,
    ) -> Result<ControlFlow<(), ()>, DatabaseImportError> {
        let database_info = match self.database_info.as_mut() {
            Some(database_info) => database_info,
            None => return Err(DatabaseImportError::DatabaseNotFoundForItems {}),
        };

        for item in items {
            let transaction = database_info.get_data_transaction(database_info.database())?;
            let (result_transaction, result) = Self::process_item(transaction, item, database_info);
            database_info.data_transaction = Some(result_transaction);
            result?;
            database_info.transaction_item_count += 1;
            if database_info.transaction_item_count % Self::COMMIT_BATCH_SIZE == 0 {
                Self::commit_write_transaction(database_info.data_transaction.take().unwrap())?;
            }
        }

        Ok(Continue(()))
    }

    async fn handle_done(&mut self) -> Result<ControlFlow<(), ()>, DatabaseImportError> {
        let database_info = match &mut self.database_info {
            Some(database_info) => database_info,
            None => return Err(DatabaseImportError::DatabaseNotFoundForDone {}),
        };

        if let Some(data_transaction) = database_info.data_transaction.take() {
            Self::commit_write_transaction(data_transaction)?;
        }

        Self::validate_imported_data(&database_info.data_info)?;
        Self::restore_relaxed_schema(database_info.database(), &database_info.schema_info)?;

        Self::send_done(&self.response_sender).await;
        self.is_done = true;
        Ok(Break(()))
    }

    async fn do_close(&mut self) {
        if let Some(mut database_info) = self.database_info.take() {
            if let Some(data_transaction) = database_info.data_transaction.take() {
                data_transaction.close();
            }

            if !self.is_done {
                let name = database_info.database().name().to_string();
                drop(database_info);
                self.database_manager.delete_database(&name).ok();
            }
        }
    }

    fn create_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseImportError> {
        self.database_manager
            .create_database(name)
            .map_err(|typedb_source| DatabaseImportError::DatabaseCreate { typedb_source })
    }

    fn get_database(&self, name: impl AsRef<str>) -> Option<Arc<Database<WALClient>>> {
        self.database_manager.database(name.as_ref())
    }

    async fn import_schema(database: Arc<Database<WALClient>>, schema: String) -> Result<(), DatabaseImportError> {
        let parsed = parse_query(schema.as_ref())
            .map_err(|typedb_source| DatabaseImportError::SchemaQueryParseFailed { typedb_source })?;
        match parsed.into_structure() {
            typeql::query::QueryStructure::Schema(schema_query) => match &schema_query {
                SchemaQuery::Define(_) => Self::execute_schema_query(database, schema_query, schema).await,
                _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
            },
            _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
        }
    }

    fn relax_schema(
        database: Arc<Database<WALClient>>,
        schema_info: &mut SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        let transaction = Self::open_schema_transaction(database)?;
        let (transaction, ()) =
            with_transaction_parts!(TransactionSchema, transaction, |inner_snapshot, type_manager, thing_manager| {
                Self::make_attribute_types_independent(
                    &mut inner_snapshot,
                    &type_manager,
                    &thing_manager,
                    schema_info,
                )?;
                Self::make_relation_types_independent(&mut inner_snapshot, &type_manager, schema_info)?;
                Self::relax_capabilities_and_cardinalities(
                    &mut inner_snapshot,
                    &type_manager,
                    &thing_manager,
                    schema_info,
                )?;
            });
        let (_, commit_result) = transaction.commit();
        commit_result.map_err(|typedb_source| DatabaseImportError::PreparationSchemaCommitFailed { typedb_source })
    }

    fn restore_relaxed_schema(
        database: Arc<Database<WALClient>>,
        schema_info: &SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        let transaction = Self::open_schema_transaction(database)?;
        let (transaction, ()) =
            with_transaction_parts!(TransactionSchema, transaction, |inner_snapshot, type_manager, thing_manager| {
                Self::restore_independent_attribute_types(&mut inner_snapshot, &type_manager, schema_info)?;
                Self::restore_independent_relation_types(&mut inner_snapshot, &type_manager, schema_info)?;
                Self::restore_capabilities_and_cardinalities(
                    &mut inner_snapshot,
                    &type_manager,
                    &thing_manager,
                    schema_info,
                )?;
            });
        let (_, commit_result) = transaction.commit();
        commit_result.map_err(|typedb_source| DatabaseImportError::FinalizationSchemaCommitFailed { typedb_source })
    }

    fn make_attribute_types_independent(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        schema_info: &mut SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        let attribute_types = type_manager
            .get_attribute_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for attribute_type in attribute_types {
            if !attribute_type
                .is_independent(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
            {
                let annotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);
                attribute_type
                    .set_annotation(snapshot, type_manager, thing_manager, annotation, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                schema_info.temporarily_independent_attribute_types.insert(attribute_type);
            }
        }
        Ok(())
    }

    fn make_relation_types_independent(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        schema_info: &mut SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        let relation_types = type_manager
            .get_relation_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for relation_type in relation_types {
            type_manager
                .set_relation_type_independent(snapshot, relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            schema_info.temporarily_independent_relation_types.insert(relation_type);
        }
        Ok(())
    }

    fn relax_capabilities_and_cardinalities(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        schema_info: &mut SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        let object_types = type_manager
            .get_object_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for object_type in object_types {
            let all_owns = object_type
                .get_owns_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for owns in all_owns.iter() {
                if Self::is_specializing_owns_with_only_cardinality_specializations(snapshot, type_manager, *owns)? {
                    let ordering = owns
                        .get_ordering(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    let annotations = owns
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    owns.owner()
                        .unset_owns(snapshot, type_manager, thing_manager, owns.attribute())
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    schema_info.temporarily_removed_specializing_owns.insert(*owns, (annotations.clone(), ordering));
                    continue;
                }

                let original_cardinality = owns
                    .get_cardinality(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                if original_cardinality != AnnotationCardinality::unchecked() {
                    match owns
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .iter()
                        .find(|annotation| {
                            matches!(annotation, OwnsAnnotation::Cardinality(_))
                                || matches!(annotation, OwnsAnnotation::Key(_))
                        }) {
                        Some(annotation) => match annotation {
                            OwnsAnnotation::Cardinality(cardinality) => {
                                schema_info.original_cardinalities_owns.insert(*owns, Some(cardinality.clone()));
                            }
                            OwnsAnnotation::Key(_) => {
                                owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Key)
                                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                                schema_info.original_keys.insert(*owns);
                            }
                            _ => unreachable!("Expected a key or a cardinality annotation"),
                        },
                        None => {
                            schema_info.original_cardinalities_owns.insert(*owns, None);
                        }
                    }
                    owns.set_annotation(
                        snapshot,
                        type_manager,
                        thing_manager,
                        OwnsAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                    )
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                }
            }

            let all_plays = object_type
                .get_plays_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for plays in all_plays.iter() {
                if Self::is_specializing_plays_with_only_cardinality_specializations(snapshot, type_manager, *plays)? {
                    let annotations = plays
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    plays
                        .player()
                        .unset_plays(snapshot, type_manager, thing_manager, plays.role())
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    schema_info.temporarily_removed_specializing_plays.insert(*plays, annotations.clone());
                    continue;
                }

                let original_cardinality = plays
                    .get_cardinality(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                if original_cardinality != AnnotationCardinality::unchecked() {
                    match plays
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .iter()
                        .find(|annotation| matches!(annotation, PlaysAnnotation::Cardinality(_)))
                    {
                        Some(annotation) => match annotation {
                            PlaysAnnotation::Cardinality(cardinality) => {
                                schema_info.original_cardinalities_plays.insert(*plays, Some(cardinality.clone()));
                            }
                            _ => unreachable!("Expected a cardinality annotation"),
                        },
                        None => {
                            schema_info.original_cardinalities_plays.insert(*plays, None);
                        }
                    }
                    plays
                        .set_annotation(
                            snapshot,
                            type_manager,
                            thing_manager,
                            PlaysAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                        )
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                }
            }

            match object_type {
                ObjectType::Entity(_) => {}
                ObjectType::Relation(relation_type) => {
                    let all_relates = relation_type
                        .get_relates_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    for relates in all_relates.iter() {
                        // all relates are exclusive for each role type

                        let original_cardinality = relates
                            .get_cardinality(snapshot, type_manager)
                            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                        if original_cardinality != AnnotationCardinality::unchecked() {
                            match relates
                                .get_annotations_declared(snapshot, type_manager)
                                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                                .iter()
                                .find(|annotation| matches!(annotation, RelatesAnnotation::Cardinality(_)))
                            {
                                Some(annotation) => match annotation {
                                    RelatesAnnotation::Cardinality(cardinality) => {
                                        schema_info
                                            .original_cardinalities_relates
                                            .insert(*relates, Some(cardinality.clone()));
                                    }
                                    _ => unreachable!("Expected a cardinality annotation"),
                                },
                                None => {
                                    schema_info.original_cardinalities_relates.insert(*relates, None);
                                }
                            }
                            relates
                                .set_annotation(
                                    snapshot,
                                    type_manager,
                                    thing_manager,
                                    RelatesAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                                )
                                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    is_specializing_with_only_cardinality_specializations_fn!(
        is_specializing_owns_with_only_cardinality_specializations,
        Owns,
        get_owned_attribute_type_constraints_cardinality
    );

    is_specializing_with_only_cardinality_specializations_fn!(
        is_specializing_plays_with_only_cardinality_specializations,
        Plays,
        get_played_role_type_constraints_cardinality
    );

    fn restore_independent_attribute_types(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        schema_info: &SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        for attribute_type in &schema_info.temporarily_independent_attribute_types {
            attribute_type
                .unset_annotation(snapshot, type_manager, AnnotationCategory::Independent)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn restore_independent_relation_types(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        schema_info: &SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        for relation_type in &schema_info.temporarily_independent_relation_types {
            type_manager
                .unset_relation_type_independent(snapshot, *relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn restore_capabilities_and_cardinalities(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        schema_info: &SchemaInfo,
    ) -> Result<(), DatabaseImportError> {
        for (owns, (annotations, ordering)) in &schema_info.temporarily_removed_specializing_owns {
            owns.owner()
                .set_owns(snapshot, type_manager, thing_manager, owns.attribute(), *ordering, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            for annotation in annotations {
                owns.set_annotation(snapshot, type_manager, thing_manager, annotation.clone())
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }
        }

        for (plays, annotations) in &schema_info.temporarily_removed_specializing_plays {
            plays
                .player()
                .set_plays(snapshot, type_manager, thing_manager, plays.role(), StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            for annotation in annotations {
                plays
                    .set_annotation(snapshot, type_manager, thing_manager, annotation.clone())
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }
        }

        for (owns, annotation) in &schema_info.original_cardinalities_owns {
            match annotation {
                Some(cardinality) => owns.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    OwnsAnnotation::Cardinality(cardinality.clone()),
                ),
                None => owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality),
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for owns in &schema_info.original_keys {
            owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            owns.set_annotation(snapshot, type_manager, thing_manager, OwnsAnnotation::Key(AnnotationKey))
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for (plays, annotation) in &schema_info.original_cardinalities_plays {
            match annotation {
                Some(cardinality) => plays.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    PlaysAnnotation::Cardinality(cardinality.clone()),
                ),
                None => plays.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality),
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for (relates, annotation) in &schema_info.original_cardinalities_relates {
            match annotation {
                Some(cardinality) => relates.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    RelatesAnnotation::Cardinality(cardinality.clone()),
                ),
                None => {
                    relates.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality)
                }
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        Ok(())
    }

    async fn execute_schema_query(
        database: Arc<Database<WALClient>>,
        query: SchemaQuery,
        source_query: String,
    ) -> Result<(), DatabaseImportError> {
        let (transaction, query_result) =
            execute_schema_query(Self::open_schema_transaction(database)?, query, source_query).await;
        query_result.map_err(|typedb_source| DatabaseImportError::SchemaQueryFailed { typedb_source })?;

        let (_, commit_result) = transaction.commit();
        commit_result.map_err(|typedb_source| DatabaseImportError::ProvidedSchemaCommitFailed { typedb_source })
    }

    fn process_item(
        transaction: TransactionWrite<WALClient>,
        item_proto: MigrationItemProto,
        database_info: &mut DatabaseInfo,
    ) -> (TransactionWrite<WALClient>, Result<(), DatabaseImportError>) {
        use typedb_protocol::migration::item;
        let MigrationItemProto { item } = item_proto;
        let Some(item) = item else {
            return (transaction, Err(DatabaseImportError::EmptyItem {}));
        };

        with_transaction_parts!(TransactionWrite, transaction, |inner_snapshot, type_manager, thing_manager| {
            match item {
                item::Item::Attribute(attribute) => {
                    let data_info = &mut database_info.data_info;
                    Self::process_attribute(&mut inner_snapshot, &type_manager, &thing_manager, attribute, data_info)
                }
                item::Item::Entity(entity) => {
                    let data_info = &mut database_info.data_info;
                    Self::process_entity(&mut inner_snapshot, &type_manager, &thing_manager, entity, data_info)
                }
                item::Item::Relation(relation) => {
                    let data_info = &mut database_info.data_info;
                    Self::process_relation(&mut inner_snapshot, &type_manager, &thing_manager, relation, data_info)
                }
                item::Item::Header(header) => Self::process_header(header, database_info),
                item::Item::Checksums(checksums) => database_info.data_info.record_client_checksums(checksums),
            }
        })
    }

    fn process_attribute(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_proto: MigrationAttributeProto,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        let MigrationAttributeProto { id, label: label_text, attributes, value } = attribute_proto;
        if !attributes.is_empty() {
            return Err(DatabaseImportError::AttributesOwningAttributes {});
        }

        let label = Label::parse_from(&label_text, None);
        let attribute_type = type_manager
            .get_attribute_type(snapshot, &label)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
            .ok_or_else(|| DatabaseImportError::UnknownAttributeType { label })?;

        debug_assert!(attribute_type.is_independent(snapshot, type_manager).unwrap());

        let value = decode_migration_value(value.ok_or_else(|| DatabaseImportError::AbsentAttributeValue {})?)
            .map_err(|typedb_source| DatabaseImportError::ConceptDecode { typedb_source })?;

        let attribute = thing_manager
            .create_attribute(snapshot, attribute_type, value)
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

        Self::fulfill_awaiting_ownerships(snapshot, thing_manager, &id, &attribute, data_info)?;

        data_info.record_attribute(id, attribute);
        Ok(())
    }

    fn process_entity(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        entity_proto: MigrationEntityProto,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        let MigrationEntityProto { id, label: label_text, attributes } = entity_proto;

        let label = Label::parse_from(&label_text, None);
        let entity_type = type_manager
            .get_entity_type(snapshot, &label)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
            .ok_or_else(|| DatabaseImportError::UnknownEntityType { label })?;

        let entity = thing_manager
            .create_entity(snapshot, entity_type)
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

        Self::process_owned_attributes(snapshot, thing_manager, entity.into_object(), attributes, data_info)?;
        Self::fulfill_awaiting_roles(snapshot, thing_manager, &id, entity.into_object(), data_info)?;

        data_info.record_entity(id, entity);
        Ok(())
    }

    fn process_relation(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation_proto: MigrationRelationProto,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        let MigrationRelationProto { id, label: label_text, attributes, roles } = relation_proto;

        let label = Label::parse_from(&label_text, None);
        let relation_type = type_manager
            .get_relation_type(snapshot, &label)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
            .ok_or_else(|| DatabaseImportError::UnknownRelationType { label })?;

        debug_assert!(type_manager.get_is_relation_type_independent(snapshot, relation_type).unwrap());

        let relation = thing_manager
            .create_relation(snapshot, relation_type)
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

        Self::process_owned_attributes(snapshot, thing_manager, relation.into_object(), attributes, data_info)?;
        Self::process_related_roles(snapshot, type_manager, thing_manager, relation, roles, data_info)?;
        Self::fulfill_awaiting_roles(snapshot, thing_manager, &id, relation.into_object(), data_info)?;

        data_info.record_relation(id, relation);
        Ok(())
    }

    fn process_owned_attributes(
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        owned_attributes: Vec<MigrationOwnedAttributeProto>,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        for MigrationOwnedAttributeProto { id } in owned_attributes {
            match data_info.attributes.instance_id_mapping.get_by_original_id(snapshot, thing_manager, &id)? {
                Some(attribute) => {
                    object
                        .set_has_unordered(snapshot, thing_manager, &attribute, StorageCounters::DISABLED)
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    data_info.record_ownership();
                }
                None => {
                    data_info.attributes.awaited_for_ownerships.entry(id).or_insert(HashSet::new()).insert(object);
                }
            }
        }
        Ok(())
    }

    fn fulfill_awaiting_ownerships(
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        original_id: &str,
        attribute: &Attribute,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        if let Some(awaiting_objects) = data_info.attributes.awaited_for_ownerships.remove(original_id) {
            for object in awaiting_objects {
                object
                    .set_has_unordered(snapshot, thing_manager, attribute, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                data_info.record_ownership();
            }
        }
        Ok(())
    }

    fn process_related_roles(
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation: Relation,
        related_roles: Vec<MigrationRoleProto>,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        for MigrationRoleProto { label: label_text, players } in related_roles {
            let label = Label::parse_from(&label_text, None);
            let role_type = type_manager
                .get_role_type(snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRoleType { label })?;

            for MigrationRolePlayerProto { id } in players {
                match data_info.objects.instance_id_mapping.get_by_original_id(snapshot, thing_manager, &id)? {
                    Some(player) => {
                        relation
                            .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                        data_info.record_role();
                    }
                    None => {
                        data_info
                            .objects
                            .awaited_for_roles
                            .entry(id)
                            .or_insert(HashSet::new())
                            .insert((role_type, relation));
                    }
                }
            }
        }
        Ok(())
    }

    fn fulfill_awaiting_roles(
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        original_id: &str,
        player: Object,
        data_info: &mut DataInfo,
    ) -> Result<(), DatabaseImportError> {
        if let Some(awaiting_relations) = data_info.objects.awaited_for_roles.remove(original_id) {
            for (role_type, relation) in awaiting_relations {
                relation
                    .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                data_info.record_role();
            }
        }
        Ok(())
    }

    fn validate_imported_data(data_info: &DataInfo) -> Result<(), DatabaseImportError> {
        if !data_info.objects.awaited_for_roles.is_empty() {
            return Err(DatabaseImportError::IncompleteRolesOnDone {
                count: data_info.objects.awaited_for_roles.len(),
            });
        }

        if !data_info.attributes.awaited_for_ownerships.is_empty() {
            return Err(DatabaseImportError::IncompleteOwnershipsOnDone {
                count: data_info.attributes.awaited_for_ownerships.len(),
            });
        }

        data_info.verify_checksums()
    }

    fn process_header(
        header_proto: MigrationHeaderProto,
        database_info: &DatabaseInfo,
    ) -> Result<(), DatabaseImportError> {
        let MigrationHeaderProto { typedb_version: original_version, original_database } = header_proto;
        let new_database = database_info.database.name();
        event!(Level::INFO, "Importing '{original_database}' from TypeDB {original_version} to '{new_database}'");
        Ok(())
    }

    async fn send_done(response_sender: &ResponseSender) {
        if let Err(err) = response_sender.send(Ok(database_import_res_done())).await {
            event!(Level::TRACE, "Submit database import done message failed: {:?}", err);
        }
    }

    async fn send_error(response_sender: &ResponseSender, status: Status) {
        if let Err(err) = response_sender.send(Err(status)).await {
            event!(Level::DEBUG, "Submit database import error message failed: {:?}", err);
        }
    }

    fn open_schema_transaction(
        database: Arc<Database<WALClient>>,
    ) -> Result<TransactionSchema<WALClient>, DatabaseImportError> {
        TransactionSchema::open(database.clone(), Self::transaction_options())
            .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source })
    }

    fn open_write_transaction(
        database: Arc<Database<WALClient>>,
    ) -> Result<TransactionWrite<WALClient>, DatabaseImportError> {
        TransactionWrite::open(database.clone(), Self::transaction_options())
            .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source })
    }

    fn commit_write_transaction(transaction: TransactionWrite<WALClient>) -> Result<(), DatabaseImportError> {
        let (_, result) = transaction.commit();
        result.map_err(|typedb_source| DatabaseImportError::DataCommitFailed { typedb_source })
    }

    fn transaction_options() -> TransactionOptions {
        TransactionOptions {
            parallel: Self::OPTIONS_PARALLEL,
            schema_lock_acquire_timeout_millis: Self::OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
            transaction_timeout_millis: Self::OPTIONS_TRANSACTION_TIMEOUT_MILLIS,
        }
    }
}
