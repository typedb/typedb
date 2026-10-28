/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fmt::{Debug, Display, Formatter},
    iter,
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, AtomicUsize},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread,
};

use bytes::{Bytes, byte_array::ByteArray};
use cache::{CacheError, SpilloverCache};
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        ThingAPI, ThingKind,
        attribute::Attribute,
        entity::Entity,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{ConceptPersistenceGuarantees, ThingManager},
    },
    type_::{
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationIndependent, AnnotationKey},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::Constraint,
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
    },
};
use concurrency::TokioTaskSpawner;
use encoding::{
    graph::{
        Typed,
        thing::{
            ThingVertex,
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_object::{ObjectID, ObjectVertex},
        },
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
    },
    value::{ValueEncodable, label::Label, value::Value, value_type::ValueType},
};
use error::{TypeDBError, typedb_error};
use query::error::QueryError;
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::StorageCounters, system_info::SystemInfo};
use serde::{Serialize, de::DeserializeOwned};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot, write::KnownToExist},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{Level, event};
use typeql::{parse_query, query::SchemaQuery};

use crate::{
    migration::{
        Checksums, MigrationOperationError,
        importing_database_handle::ImportingDatabaseHandle,
        item::{
            AttributeByValue, AttributeItem, EntityItem, Guarantees, ItemOrder, MigrationItem, OwnedAttribute, Player,
            PlayerByLabelScopedId, RelationItem, RolePlayers,
        },
    },
    query::execute_schema_query,
    transaction::{TransactionError, TransactionSchema, TransactionWrite},
    with_transaction_parts,
};

macro_rules! is_specializing_fn {
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
            // If this capability is affected by multiple cardinality constraints from the same interface type, then
            // the object type has multiple ownerships of this interface type: some inherited and one declared (specializing)
            let same_interface_type_count = cardinalities
                .into_iter()
                .filter(|constraint| constraint.source().interface() == interface_type)
                .count();
            Ok(same_interface_type_count > 1)
        }
    };
}

macro_rules! for_item_in_write_transaction {
    (
        $self:ident, |$inner_snapshot:ident, $type_manager:ident, $thing_manager:ident| $expr:expr
    ) => {{
        let transaction = $self.get_data_transaction()?;
        let (transaction, result) = with_transaction_parts!(
            TransactionWrite,
            transaction,
            |$inner_snapshot, $type_manager, $thing_manager, _fm, _qm| { $expr }
        );

        $self.data_transaction = Some(transaction);
        result?;
        $self.count_item();
        let pending_writes = $self.data_transaction.as_ref().unwrap().snapshot.operations().len();
        if pending_writes >= $self.configuration.max_writes_per_commit {
            let transaction = $self.data_transaction.take().unwrap();
            $self.commit_write_transaction(transaction)?;
        }
        Ok(())
    }};
}

#[derive(Debug)]
struct SchemaInfo {
    temporarily_independent_attribute_types: HashSet<AttributeType>,
    temporarily_non_independent_attribute_types: HashSet<AttributeType>,
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
            temporarily_non_independent_attribute_types: HashSet::new(),
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

type IID = ByteArray<BUFFER_KEY_INLINE>;

#[derive(Debug)]
struct DataInfo {
    objects: RwLock<ObjectsInfo>,
    attributes: RwLock<AttributesInfo>,
    checksums: Checksums,
    expected_checksums: OnceLock<Checksums>,
    unresolved_ownerships: AtomicUsize,
    unresolved_role_players: AtomicUsize,
}

impl DataInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            objects: RwLock::new(ObjectsInfo::new(&cache_directory, database_name)),
            attributes: RwLock::new(AttributesInfo::new(cache_directory, database_name)),
            checksums: Checksums::new(),
            expected_checksums: OnceLock::new(),
            unresolved_ownerships: AtomicUsize::new(0),
            unresolved_role_players: AtomicUsize::new(0),
        }
    }

    fn count_entity(&self) {
        self.checksums.increment_entity_count();
    }

    fn count_relation(&self) {
        self.checksums.increment_relation_count();
    }

    fn count_attribute(&self) {
        self.checksums.increment_attribute_count();
    }

    fn record_entity(&self, original_id: String, entity: Entity) -> Result<(), DatabaseImportError> {
        self.objects.write().unwrap().instance_id_mapping.record(original_id, entity.into_object())
    }

    fn record_relation(&self, original_id: String, relation: Relation) -> Result<(), DatabaseImportError> {
        self.objects.write().unwrap().instance_id_mapping.record(original_id, relation.into_object())
    }

    fn record_attribute(&self, original_id: String, attribute: Attribute) -> Result<(), DatabaseImportError> {
        self.attributes.write().unwrap().instance_id_mapping.record(original_id, attribute)
    }

    fn record_ownership(&self) {
        self.checksums.increment_ownership_count();
    }

    fn record_role(&self) {
        self.checksums.increment_role_count();
    }

    fn get_recorded_object(&self, id: &str) -> Result<Option<Object>, DatabaseImportError> {
        match self.objects.read().unwrap().instance_id_mapping.get_iid_by_original_id(id)? {
            Some(iid) => Ok(Some(Object::new(decode_object_vertex(&iid)?))),
            None => Ok(None),
        }
    }

    fn get_attribute_by_original_id(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        id: &str,
    ) -> Result<Option<Attribute>, DatabaseImportError> {
        let Some(recorded_iid) = self.attributes.read().unwrap().instance_id_mapping.get_iid_by_original_id(id)? else {
            return Ok(None);
        };
        thing_manager
            .get_instance::<Attribute>(snapshot, &Bytes::Array(recorded_iid), StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })
    }

    fn append_deferred_ownership(&self, deferred: DeferredOwnership) -> Result<(), DatabaseImportError> {
        self.attributes.write().unwrap().deferred_ownerships.append(deferred)
    }

    fn append_deferred_links(&self, deferred: DeferredLinks) -> Result<(), DatabaseImportError> {
        self.objects.write().unwrap().deferred_links.append(deferred)
    }

    fn take_deferred_ownerships(&self) -> Option<SpilloverCache<DeferredOwnership>> {
        self.attributes.write().unwrap().deferred_ownerships.take()
    }

    fn take_deferred_links(&self) -> Option<SpilloverCache<DeferredLinks>> {
        self.objects.write().unwrap().deferred_links.take()
    }

    fn record_unresolved_ownership(&self) {
        self.unresolved_ownerships.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    fn record_unresolved_role_player(&self) {
        self.unresolved_role_players.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    fn unresolved_ownerships(&self) -> usize {
        self.unresolved_ownerships.load(core::sync::atomic::Ordering::Relaxed)
    }

    fn unresolved_role_players(&self) -> usize {
        self.unresolved_role_players.load(core::sync::atomic::Ordering::Relaxed)
    }

    fn has_expected_checksums(&self) -> bool {
        self.expected_checksums.get().is_some()
    }

    fn record_expected_checksums(&self, checksums: Checksums) -> Result<(), DatabaseImportError> {
        self.expected_checksums.set(checksums).map_err(|_| DatabaseImportError::DuplicateClientChecksums {})
    }

    fn verify_checksums(&self) -> Result<(), DatabaseImportError> {
        match self.expected_checksums.get() {
            Some(client_checksums) => {
                if &self.checksums == client_checksums {
                    Ok(())
                } else {
                    let details = format!("client expected {client_checksums}, but recorded {}", &self.checksums);
                    Err(DatabaseImportError::InvalidChecksumsOnDone { details })
                }
            }
            None => Err(DatabaseImportError::NoClientChecksumsOnDone {}),
        }
    }
}

#[derive(Debug)]
struct InstanceIDMapping<T: ThingAPI> {
    original_to_buffered_ids: SpilloverCache<IID>,
    _phantom_data: PhantomData<T>,
}

impl<T: ThingAPI> InstanceIDMapping<T> {
    const CACHE_SPILLOVER_THRESHOLD: usize = 300_000;

    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            original_to_buffered_ids: SpilloverCache::new(
                cache_directory,
                Some(database_name),
                Self::CACHE_SPILLOVER_THRESHOLD,
            ),
            _phantom_data: PhantomData,
        }
    }

    fn get_iid_by_original_id(&self, id: &str) -> Result<Option<IID>, DatabaseImportError> {
        self.original_to_buffered_ids.get(id).map_err(|source| DatabaseImportError::CacheError { source })
    }

    fn record(&mut self, original_id: String, instance: T) -> Result<(), DatabaseImportError> {
        self.original_to_buffered_ids
            .insert(original_id, instance.iid().into_array())
            .map_err(|source| DatabaseImportError::CacheError { source })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeferredOwnership {
    attribute_original_id: String,
    owner_iid: IID,
}

/// All links of one relation, deferred together so that they are written in one transaction
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeferredLinks {
    relation_iid: IID,
    links: Vec<DeferredLink>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeferredLink {
    role_type_id: TypeIDUInt,
    player: DeferredPlayer,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum DeferredPlayer {
    ByOriginalId(String),
    ByIid(IID),
}

enum ResolvedPlayer {
    Player(Object),
    Missing(String),
}

enum ImportConcept {
    Entity(EntityItem),
    Relation(RelationItem),
    Attribute(AttributeItem),
    DeferredOwnership(DeferredOwnership),
    DeferredLinks(DeferredLinks),
}

impl ImportConcept {
    fn phase(&self, item_order: ItemOrder) -> ImportPhase {
        match self {
            Self::Entity(_) => ImportPhase::phase_for(ThingKind::Entity, item_order),
            Self::Relation(_) => ImportPhase::phase_for(ThingKind::Relation, item_order),
            Self::Attribute(_) => ImportPhase::phase_for(ThingKind::Attribute, item_order),
            Self::DeferredOwnership(_) | Self::DeferredLinks(_) => ImportPhase::Deferred,
        }
    }
}

#[derive(Debug)]
struct SpilloverLog<T: Serialize + DeserializeOwned + Clone> {
    records: Option<SpilloverCache<T>>,
    next_sequence: u64,
}

impl<T: Serialize + DeserializeOwned + Clone> SpilloverLog<T> {
    const CACHE_SPILLOVER_THRESHOLD: usize = 300_000;

    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            records: Some(SpilloverCache::new(cache_directory, Some(database_name), Self::CACHE_SPILLOVER_THRESHOLD)),
            next_sequence: 0,
        }
    }

    fn append(&mut self, record: T) -> Result<(), DatabaseImportError> {
        let key = format!("{:020}", self.next_sequence);
        self.next_sequence += 1;
        self.records
            .as_mut()
            .expect("Deferred records are only appended before the import stream is done")
            .insert(key, record)
            .map_err(|source| DatabaseImportError::CacheError { source })
    }

    fn take(&mut self) -> Option<SpilloverCache<T>> {
        self.records.take()
    }
}

#[derive(Debug)]
struct ObjectsInfo {
    pub instance_id_mapping: InstanceIDMapping<Object>,
    pub deferred_links: SpilloverLog<DeferredLinks>,
}

impl ObjectsInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            deferred_links: SpilloverLog::new(cache_directory, database_name),
        }
    }
}

#[derive(Debug)]
struct AttributesInfo {
    pub instance_id_mapping: InstanceIDMapping<Attribute>,
    pub deferred_ownerships: SpilloverLog<DeferredOwnership>,
}

impl AttributesInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            deferred_ownerships: SpilloverLog::new(cache_directory, database_name),
        }
    }
}

type ImportItems = Box<dyn Iterator<Item = Result<MigrationItem, MigrationOperationError>> + Send>;

enum ImportMessage {
    Schema(String),
    Items(ImportItems),
    Finalize,
}

type ImportResult = Result<u64, DatabaseImportError>;

pub struct DatabaseImporterHandle {
    message_sender: Option<mpsc::Sender<ImportMessage>>,
    error_state: ErrorState,
    join_handle: Option<JoinHandle<ImportResult>>,
}

impl DatabaseImporterHandle {
    pub async fn send_schema(&mut self, schema: String) -> Result<(), DatabaseImportError> {
        self.send_message(ImportMessage::Schema(schema)).await
    }

    pub async fn send(&mut self, item: MigrationItem) -> Result<(), DatabaseImportError> {
        self.send_message(ImportMessage::Items(Box::new(iter::once(Ok(item))))).await
    }

    pub async fn send_batch<E>(
        &mut self,
        items: impl IntoIterator<Item = Result<MigrationItem, E>, IntoIter: Send + 'static>,
    ) -> Result<(), DatabaseImportError>
    where
        E: TypeDBError + Send + Sync + 'static,
    {
        let items = items.into_iter().map(|item| item.map_err(|error| Arc::new(error) as MigrationOperationError));
        self.send_message(ImportMessage::Items(Box::new(items))).await
    }

    async fn send_message(&mut self, message: ImportMessage) -> Result<(), DatabaseImportError> {
        let sent = match &self.message_sender {
            Some(sender) => sender.send(message).await.is_ok(),
            None => false,
        };
        if sent { Ok(()) } else { Err(Self::stop_reason(self.join().await)) }
    }

    pub async fn finalize(&mut self) -> ImportResult {
        if let Some(sender) = self.message_sender.take() {
            // If this fails, the importer has already stopped and its result carries the reason.
            let _ = sender.send(ImportMessage::Finalize).await;
        }
        self.join().await
    }

    pub async fn abort(mut self) -> ImportResult {
        self.interrupt();
        self.join().await
    }

    fn interrupt(&mut self) {
        self.error_state.stop_with_error(DatabaseImportError::Interrupted {});
        // dropping sender closes channel to wake up receiver if required
        self.message_sender = None;
    }

    /// Waits for the importer task to finish, e.g. after failing. Only the first completed wait gets its result:
    /// later ones return `ImporterStopped`.
    pub async fn join(&mut self) -> ImportResult {
        // The join handle is only cleared once the task ends, so a cancelled (dropped) wait can be retried.
        let Some(join_handle) = self.join_handle.as_mut() else {
            return Err(DatabaseImportError::ImporterStopped {});
        };
        let result = join_handle.await.unwrap_or_else(|err| {
            event!(Level::ERROR, "The importer task failed: {err}");
            Err(DatabaseImportError::ImporterStopped {})
        });
        self.join_handle = None;
        result
    }

    fn stop_reason(result: ImportResult) -> DatabaseImportError {
        match result {
            Ok(_) => DatabaseImportError::ImporterStopped {},
            Err(err) => err,
        }
    }
}

impl Drop for DatabaseImporterHandle {
    fn drop(&mut self) {
        if self.join_handle.is_some() {
            event!(Level::WARN, "An import was dropped before it was finalised or aborted. Aborting the import.");
            self.interrupt();
        }
    }
}

impl std::fmt::Debug for DatabaseImporterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseImporterHandle").finish_non_exhaustive()
    }
}

pub struct DatabaseImporter {
    importing_database: Option<Arc<dyn ImportingDatabaseHandle>>,
    database_name: String,
    configuration: ImporterConfiguration,
    schema_info: SchemaInfo,
    data_info: Arc<DataInfo>,
    total_item_count: Arc<AtomicU64>,
    error_state: ErrorState,

    schema_imported: bool,
    phase: Option<DatabaseImporterPhase>,
}

impl Debug for DatabaseImporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseImporter").field("database_name", &self.database_name).finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ImporterConfiguration {
    scratch_directory: PathBuf,
    max_writes_per_commit: usize,
    buffer_size: usize,
    worker_count: usize,
    item_order: ItemOrder,
    attributes_unique: bool,
    players_by_label_scoped_ids: bool,
    owned_attributes_by_value: bool,
}

impl ImporterConfiguration {
    const MAX_WRITES_PER_COMMIT: usize = 10_000;

    pub fn new(scratch_directory: PathBuf) -> Self {
        Self {
            scratch_directory,
            max_writes_per_commit: Self::MAX_WRITES_PER_COMMIT,
            buffer_size: 48,
            worker_count: 1,
            item_order: ItemOrder::Unspecified,
            attributes_unique: false,
            players_by_label_scoped_ids: false,
            owned_attributes_by_value: false,
        }
    }

    pub fn with_buffer_size(self, buffer_size: usize) -> Self {
        Self { buffer_size, ..self }
    }

    pub fn with_max_writes_per_commit(self, max_writes_per_commit: usize) -> Self {
        Self { max_writes_per_commit, ..self }
    }

    pub fn with_guarantees(self, guarantees: Guarantees) -> Self {
        let Guarantees { ordering, attributes_unique, players_by_label_scoped_ids, owned_attributes_by_value } =
            guarantees;
        let worker_count = match ordering {
            ItemOrder::Unspecified => 1,
            ItemOrder::AttributeEntityRelation => SystemInfo::get_cpu_count().saturating_sub(1).max(1),
        };
        Self {
            worker_count,
            item_order: ordering,
            attributes_unique,
            players_by_label_scoped_ids,
            owned_attributes_by_value,
            ..self
        }
    }

    fn as_guarantees(&self) -> Guarantees {
        Guarantees {
            ordering: self.item_order,
            attributes_unique: self.attributes_unique,
            players_by_label_scoped_ids: self.players_by_label_scoped_ids,
            owned_attributes_by_value: self.owned_attributes_by_value,
        }
    }

    fn worker_count(&self) -> usize {
        self.worker_count
    }

    fn use_external_label_scoped_ids(&self) -> bool {
        // object ids are only needed to resolve players by id: with label-scoped ids and the ordering guarantee
        // that players exist, they are never consulted
        matches!(self.item_order, ItemOrder::AttributeEntityRelation) && self.players_by_label_scoped_ids
    }

    fn skip_recording_of_inline_attributes(&self) -> bool {
        // inline attribute ids are only needed to resolve ownerships by id: when ownerships carry the value
        // and the ordering guarantees the attribute exists, they are never consulted
        matches!(self.item_order, ItemOrder::AttributeEntityRelation) && self.owned_attributes_by_value
    }

    fn skip_predecessor_phase_existence_checks(&self) -> bool {
        matches!(self.item_order, ItemOrder::AttributeEntityRelation)
    }
}

impl DatabaseImporter {
    const DRAIN_CHUNK_SIZE: usize = 10_000;

    pub fn start(
        importing_database: Arc<dyn ImportingDatabaseHandle>,
        importer_configuration: ImporterConfiguration,
        task_spawner: &TokioTaskSpawner,
    ) -> DatabaseImporterHandle {
        let (sender, receiver) = mpsc::channel(importer_configuration.buffer_size);
        let error_state = ErrorState::default();
        let database_name = importing_database.database_name().to_owned();
        let data_info = Arc::new(DataInfo::new(&importer_configuration.scratch_directory, &database_name));
        let importer = Self {
            importing_database: Some(importing_database),
            database_name,
            configuration: importer_configuration,
            schema_info: SchemaInfo::new(),
            data_info,
            total_item_count: Arc::new(AtomicU64::new(0)),
            error_state: error_state.clone(),
            schema_imported: false,
            phase: None,
        };
        let join_handle = task_spawner.spawn_blocking(move || importer.listen(receiver));
        DatabaseImporterHandle { message_sender: Some(sender), error_state, join_handle: Some(join_handle) }
    }

    fn listen(mut self, mut receiver: mpsc::Receiver<ImportMessage>) -> ImportResult {
        loop {
            self.check_stopped()?;
            match receiver.blocking_recv() {
                None => return Err(self.error_state.error().unwrap_or(DatabaseImportError::Interrupted {})),
                Some(ImportMessage::Schema(_)) if self.schema_imported => {
                    return Err(DatabaseImportError::SchemaAlreadyImported {});
                }
                Some(ImportMessage::Schema(schema)) => {
                    self.import_schema(schema)?;
                    self.schema_imported = true;
                }
                Some(ImportMessage::Items(_)) if !self.schema_imported => {
                    return Err(DatabaseImportError::ItemBeforeSchema {});
                }
                Some(ImportMessage::Items(items)) => {
                    for item in items {
                        self.check_stopped()?;
                        self.apply(item.map_err(|typedb_source| DatabaseImportError::ItemDecode { typedb_source })?)?;
                    }
                }
                Some(ImportMessage::Finalize) => {
                    self.finish_phase()?;
                    let total_item_count = self.total_item_count();
                    return self.finalize().map(|()| total_item_count);
                }
            }
        }
    }

    fn check_stopped(&self) -> Result<(), DatabaseImportError> {
        match self.error_state.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn apply(&mut self, item: MigrationItem) -> Result<(), DatabaseImportError> {
        match item {
            MigrationItem::Checksums(checksums) => self.data_info.record_expected_checksums(checksums),
            _ if self.data_info.has_expected_checksums() => Err(DatabaseImportError::ItemAfterChecksums {}),
            MigrationItem::Header { typedb_version, original_database, guarantees } => {
                if self.phase.is_some() {
                    return Err(DatabaseImportError::HeaderAfterItems {});
                }
                self.configuration = self.configuration.clone().with_guarantees(guarantees);
                event!(
                    Level::DEBUG,
                    "Importing '{original_database}' from TypeDB {typedb_version} to '{}' with {:?}.",
                    self.database_name,
                    self.configuration.as_guarantees()
                );
                Ok(())
            }
            MigrationItem::Entity(entity) => self.import_concept(ImportConcept::Entity(entity)),
            MigrationItem::Relation(relation) => self.import_concept(ImportConcept::Relation(relation)),
            MigrationItem::Attribute(attribute) => self.import_concept(ImportConcept::Attribute(attribute)),
        }
    }

    fn import_concept(&mut self, concept: ImportConcept) -> Result<(), DatabaseImportError> {
        let phase = concept.phase(self.configuration.item_order);
        match self.phase.as_ref().map(|current| current.phase) {
            None => self.phase = Some(DatabaseImporterPhase::start(self, phase)?),
            Some(current_phase) if current_phase != phase => {
                current_phase.validate_transition(phase, self.configuration.item_order)?;
                self.finish_phase()?;
                self.phase = Some(DatabaseImporterPhase::start(self, phase)?);
            }
            Some(_) => (),
        }
        self.phase.as_mut().expect("a phase runs while importing").import_concept(concept)
    }

    fn finish_phase(&mut self) -> Result<(), DatabaseImportError> {
        self.phase.take().map_or(Ok(()), DatabaseImporterPhase::finish)
    }

    fn import_schema(&mut self, schema: String) -> Result<(), DatabaseImportError> {
        if schema.trim().is_empty() {
            return Ok(());
        }
        self.submit_original_schema(schema)?;
        self.relax_schema()
    }

    fn finalize(&mut self) -> Result<(), DatabaseImportError> {
        self.check_stopped()?;
        self.import_deferred_ownerships()?;
        self.check_stopped()?;
        self.import_deferred_links()?;
        self.check_stopped()?;
        self.validate_imported_data()?;
        self.check_stopped()?;
        self.restore_relaxed_schema()?;
        self.check_stopped()?;

        let Some(importing_database) = self.importing_database.take() else {
            return Err(DatabaseImportError::DoubleFinalisation { name: self.database_name().to_string() });
        };

        match importing_database.try_unwrap() {
            Ok(importing_database) => importing_database
                .finalise()
                .map_err(|typedb_source| DatabaseImportError::FinalisationFailed { typedb_source }),
            Err(_) => Err(DatabaseImportError::ImportingDatabaseStillShared {}),
        }
    }

    fn import_deferred_ownerships(&mut self) -> Result<(), DatabaseImportError> {
        let records = self.data_info.take_deferred_ownerships();
        self.import_deferred(records, "ownerships", ImportConcept::DeferredOwnership)?;
        match self.data_info.unresolved_ownerships() {
            0 => Ok(()),
            count => Err(DatabaseImportError::IncompleteOwnershipsOnDone { count }),
        }
    }

    fn import_deferred_links(&mut self) -> Result<(), DatabaseImportError> {
        let records = self.data_info.take_deferred_links();
        self.import_deferred(records, "relations' links", ImportConcept::DeferredLinks)?;
        match self.data_info.unresolved_role_players() {
            0 => Ok(()),
            count => Err(DatabaseImportError::IncompleteRolesOnDone { count }),
        }
    }

    fn import_deferred<T>(
        &mut self,
        records: Option<SpilloverCache<T>>,
        deferred_name: &str,
        to_import_concept: impl Fn(T) -> ImportConcept,
    ) -> Result<(), DatabaseImportError>
    where
        T: Serialize + DeserializeOwned + Clone,
    {
        let Some(records) = records else { return Ok(()) };
        let mut written = 0u64;
        for chunk in records.into_chunks(Self::DRAIN_CHUNK_SIZE) {
            self.check_stopped()?;
            for (_key, deferred) in chunk.map_err(|source| DatabaseImportError::CacheError { source })? {
                self.import_concept(to_import_concept(deferred))?;
                written += 1;
            }
        }
        self.finish_phase()?;
        if written > 0 {
            event!(
                Level::DEBUG,
                "Wrote {written} {deferred_name} deferred until their referenced instances were imported."
            );
        }
        Ok(())
    }

    fn database_name(&self) -> &str {
        &self.database_name
    }

    fn total_item_count(&self) -> u64 {
        self.total_item_count.load(core::sync::atomic::Ordering::Relaxed)
    }

    fn submit_original_schema(&self, schema: String) -> Result<(), DatabaseImportError> {
        let parsed = parse_query(schema.as_ref())
            .map_err(|typedb_source| DatabaseImportError::SchemaQueryParseFailed { typedb_source })?;
        match parsed.into_structure() {
            typeql::query::QueryStructure::Schema(schema_query) => match &schema_query {
                SchemaQuery::Define(_) => {
                    let transaction = self.open_schema_transaction()?;
                    let (transaction, query_result) = execute_schema_query(transaction, schema_query, schema);
                    query_result.map_err(|typedb_source| DatabaseImportError::SchemaQueryFailed { typedb_source })?;
                    self.commit_schema_transaction(transaction, |typedb_source| {
                        DatabaseImportError::ProvidedSchemaCommitFailed { typedb_source }
                    })
                }
                _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
            },
            _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
        }
    }

    fn relax_schema(&mut self) -> Result<(), DatabaseImportError> {
        let transaction = self.open_schema_transaction()?;
        let (transaction, ()) = with_transaction_parts!(
            TransactionSchema,
            transaction,
            |inner_snapshot, type_manager, thing_manager, _fm, _qm| {
                self.make_attribute_types_independent(&mut inner_snapshot, &type_manager, &thing_manager)?;
                self.check_stopped()?;
                self.make_relation_types_independent(&mut inner_snapshot, &type_manager)?;
                self.check_stopped()?;
                self.relax_capabilities_and_cardinalities(&mut inner_snapshot, &type_manager, &thing_manager)?;
            }
        );

        self.check_stopped()?;
        self.commit_schema_transaction(transaction, |typedb_source| {
            DatabaseImportError::PreparationSchemaCommitFailed { typedb_source }
        })
    }

    fn restore_relaxed_schema(&mut self) -> Result<(), DatabaseImportError> {
        let transaction = self.open_schema_transaction()?;
        let (transaction, ()) = with_transaction_parts!(
            TransactionSchema,
            transaction,
            |inner_snapshot, type_manager, thing_manager, _fm, _qm| {
                self.restore_independent_attribute_types(&mut inner_snapshot, &type_manager, &thing_manager)?;
                self.check_stopped()?;
                self.restore_independent_relation_types(&mut inner_snapshot, &type_manager)?;
                self.check_stopped()?;
                self.restore_capabilities_and_cardinalities(&mut inner_snapshot, &type_manager, &thing_manager)?;
            }
        );

        self.check_stopped()?;
        self.commit_schema_transaction(transaction, |typedb_source| {
            DatabaseImportError::FinalizationSchemaCommitFailed { typedb_source }
        })
    }

    // The @independent constraint is inherited by all subtypes, and a subtype cannot redeclare
    // an inherited constraint. To make every attribute type independent, the annotation is
    // declared on root types only, and existing subtype declarations are temporarily undeclared
    // so they do not become redundant
    fn make_attribute_types_independent(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let attribute_types = type_manager
            .get_attribute_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for attribute_type in attribute_types {
            let is_root = attribute_type
                .get_supertype(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .is_none();
            let annotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);
            let is_declared_independent = attribute_type
                .get_annotations_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .contains(&annotation);
            if is_root && !is_declared_independent {
                attribute_type
                    .set_annotation(snapshot, type_manager, thing_manager, annotation, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.schema_info.temporarily_independent_attribute_types.insert(attribute_type);
            } else if !is_root && is_declared_independent {
                attribute_type
                    .unset_annotation(snapshot, type_manager, AnnotationCategory::Independent)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.schema_info.temporarily_non_independent_attribute_types.insert(attribute_type);
            }
        }
        Ok(())
    }

    fn make_relation_types_independent(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), DatabaseImportError> {
        let relation_types = type_manager
            .get_relation_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for relation_type in relation_types {
            type_manager
                .set_relation_type_independent(snapshot, relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            self.schema_info.temporarily_independent_relation_types.insert(relation_type);
        }
        Ok(())
    }

    fn relax_capabilities_and_cardinalities(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let schema_info = &mut self.schema_info;
        let object_types = type_manager
            .get_object_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for object_type in object_types {
            let all_owns = object_type
                .get_owns_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for owns in all_owns.iter() {
                if Self::is_specializing_owns(snapshot, type_manager, *owns)? {
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
                if original_cardinality.start() == 0 {
                    continue;
                }
                match owns
                    .get_annotations_declared(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                    .iter()
                    .find(|annotation| {
                        matches!(annotation, OwnsAnnotation::Cardinality(_))
                            || matches!(annotation, OwnsAnnotation::Key(_))
                    }) {
                    Some(OwnsAnnotation::Cardinality(cardinality)) => {
                        schema_info.original_cardinalities_owns.insert(*owns, Some(*cardinality));
                    }
                    Some(OwnsAnnotation::Key(_)) => {
                        owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Key)
                            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                        schema_info.original_keys.insert(*owns);
                        continue;
                    }
                    Some(_) => unreachable!("Expected a key or a cardinality annotation"),
                    None => {
                        schema_info.original_cardinalities_owns.insert(*owns, None);
                    }
                }
                owns.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    OwnsAnnotation::Cardinality(Self::relaxed(original_cardinality)),
                )
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }

            let all_plays = object_type
                .get_plays_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for plays in all_plays.iter() {
                if Self::is_specializing_plays(snapshot, type_manager, *plays)? {
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
                if original_cardinality.start() == 0 {
                    continue;
                }
                match plays
                    .get_annotations_declared(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                    .iter()
                    .find(|annotation| matches!(annotation, PlaysAnnotation::Cardinality(_)))
                {
                    Some(PlaysAnnotation::Cardinality(cardinality)) => {
                        schema_info.original_cardinalities_plays.insert(*plays, Some(*cardinality));
                    }
                    Some(_) => unreachable!("Expected a cardinality annotation"),
                    None => {
                        schema_info.original_cardinalities_plays.insert(*plays, None);
                    }
                }
                plays
                    .set_annotation(
                        snapshot,
                        type_manager,
                        thing_manager,
                        PlaysAnnotation::Cardinality(Self::relaxed(original_cardinality)),
                    )
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }

            match object_type {
                ObjectType::Entity(_) => {}
                ObjectType::Relation(relation_type) => {
                    let all_relates = relation_type
                        .get_relates_explicit_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    for relates in all_relates.iter() {
                        let original_cardinality = relates
                            .get_cardinality(snapshot, type_manager)
                            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                        if original_cardinality.start() == 0 {
                            continue;
                        }
                        match relates
                            .get_annotations_declared(snapshot, type_manager)
                            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                            .iter()
                            .find(|annotation| matches!(annotation, RelatesAnnotation::Cardinality(_)))
                        {
                            Some(RelatesAnnotation::Cardinality(cardinality)) => {
                                schema_info.original_cardinalities_relates.insert(*relates, Some(*cardinality));
                            }
                            Some(_) => unreachable!("Expected a cardinality annotation"),
                            None => {
                                schema_info.original_cardinalities_relates.insert(*relates, None);
                            }
                        }
                        relates
                            .set_annotation(
                                snapshot,
                                type_manager,
                                thing_manager,
                                RelatesAnnotation::Cardinality(Self::relaxed(original_cardinality)),
                            )
                            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    }
                }
            }
        }
        Ok(())
    }

    is_specializing_fn!(is_specializing_owns, Owns, get_owned_attribute_type_constraints_cardinality);

    is_specializing_fn!(is_specializing_plays, Plays, get_played_role_type_constraints_cardinality);

    fn restore_independent_attribute_types(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        for attribute_type in &self.schema_info.temporarily_independent_attribute_types {
            attribute_type
                .unset_annotation(snapshot, type_manager, AnnotationCategory::Independent)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        for attribute_type in &self.schema_info.temporarily_non_independent_attribute_types {
            let annotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);
            attribute_type
                .set_annotation(snapshot, type_manager, thing_manager, annotation, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn restore_independent_relation_types(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), DatabaseImportError> {
        for relation_type in &self.schema_info.temporarily_independent_relation_types {
            type_manager
                .unset_relation_type_independent(snapshot, *relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn relaxed(cardinality: AnnotationCardinality) -> AnnotationCardinality {
        AnnotationCardinality::new(0, cardinality.end())
    }

    fn restore_capabilities_and_cardinalities(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let schema_info = &self.schema_info;
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

    fn validate_imported_data(&self) -> Result<(), DatabaseImportError> {
        self.data_info.verify_checksums()
    }

    fn open_schema_transaction(&self) -> Result<TransactionSchema<WALClient>, DatabaseImportError> {
        self.importing_database()?
            .open_schema()
            .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source })
    }

    fn commit_schema_transaction(
        &self,
        transaction: TransactionSchema<WALClient>,
        map_err: impl Fn(MigrationOperationError) -> DatabaseImportError,
    ) -> Result<(), DatabaseImportError> {
        let (_, finalise_result) = transaction.finalise();
        let intent = finalise_result.map_err(|source| map_err(Arc::new(source)))?;
        self.importing_database()?.commit_schema(intent).map_err(map_err)
    }

    fn importing_database(&self) -> Result<&dyn ImportingDatabaseHandle, DatabaseImportError> {
        self.importing_database.as_deref().ok_or(DatabaseImportError::AccessAfterFinalisation {})
    }
}

fn decode_object_vertex(iid: &[u8]) -> Result<ObjectVertex, DatabaseImportError> {
    ObjectVertex::try_decode(iid).ok_or_else(|| DatabaseImportError::ConceptRead {
        typedb_source: Box::new(ConceptReadError::IidRepresentsWrongInstanceKind {}),
    })
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ImportPhase {
    Default,
    Attributes,
    Entities,
    Relations,
    Deferred,
}

impl ImportPhase {
    fn concept_persistence_guarantees(
        &self,
        configuration: &ImporterConfiguration,
    ) -> Option<Box<dyn ConceptPersistenceGuarantees>> {
        match configuration.item_order {
            ItemOrder::Unspecified => None,
            ItemOrder::AttributeEntityRelation => Some(Box::new(AttributeEntityRelationConceptPersistenceGuarantees {
                phase: *self,
                attributes_unique: configuration.attributes_unique,
            })),
        }
    }

    fn phase_for(thing_kind: ThingKind, item_order: ItemOrder) -> Self {
        match item_order {
            ItemOrder::Unspecified => Self::Default,
            ItemOrder::AttributeEntityRelation => match thing_kind {
                ThingKind::Entity => Self::Entities,
                ThingKind::Relation => Self::Relations,
                ThingKind::Attribute => Self::Attributes,
            },
        }
    }

    fn validate_transition(&self, next_phase: Self, item_order: ItemOrder) -> Result<(), DatabaseImportError> {
        match item_order {
            ItemOrder::Unspecified => match (self, next_phase) {
                (Self::Default, Self::Default | Self::Deferred) => Ok(()),
                (_, _) => Err(DatabaseImportError::UnexpectedPhase { order: item_order, phase: next_phase }),
            },
            ItemOrder::AttributeEntityRelation => match (self, next_phase) {
                (Self::Attributes, Self::Attributes | Self::Entities | Self::Relations | Self::Deferred) => Ok(()),
                (Self::Attributes, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Entities, Self::Entities | Self::Relations | Self::Deferred) => Ok(()),
                (Self::Entities, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Relations, Self::Relations | Self::Deferred) => Ok(()),
                (Self::Relations, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Deferred, Self::Deferred) => Ok(()),
                (Self::Deferred, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Default, _) => {
                    Err(DatabaseImportError::UnexpectedPhase { order: item_order, phase: next_phase })
                }
            },
        }
    }

    fn is_from_previous_phase(&self, thing_kind: ThingKind, item_order: ItemOrder) -> bool {
        let thing_phase = Self::phase_for(thing_kind, item_order);
        match item_order {
            ItemOrder::Unspecified => false,
            ItemOrder::AttributeEntityRelation => match (self, thing_phase) {
                (Self::Attributes, _) => false,

                (Self::Entities, Self::Attributes) => true,
                (Self::Entities, _) => false,

                (Self::Relations, Self::Entities | Self::Attributes) => true,
                (Self::Relations, _) => false,

                (Self::Deferred, Self::Entities | Self::Attributes | Self::Relations) => true,
                (Self::Deferred, _) => false,

                (Self::Default, _) => false,
            },
        }
    }
}

impl Display for ImportPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

struct DatabaseImporterPhase {
    phase: ImportPhase,
    sender: Option<SyncSender<Vec<ImportConcept>>>,
    batch_builder: Vec<ImportConcept>,
    workers: Vec<thread::JoinHandle<()>>,
    error_state: ErrorState,
}

#[derive(Default, Clone)]
struct ErrorState {
    state: Arc<OnceLock<DatabaseImportError>>,
}

impl ErrorState {
    fn stop_with_error(&self, error: DatabaseImportError) {
        let _ = self.state.set(error);
    }

    fn is_errored(&self) -> bool {
        self.state.get().is_some()
    }

    fn error(&self) -> Option<DatabaseImportError> {
        self.state.get().cloned()
    }
}

impl DatabaseImporterPhase {
    const QUEUE_SIZE_MULTIPLIER: usize = 4;
    const BATCH_SIZE: usize = 1_000;

    fn start(importer: &DatabaseImporter, import_phase: ImportPhase) -> Result<Self, DatabaseImportError> {
        let (sender, receiver) = sync_channel(importer.configuration.worker_count() * Self::QUEUE_SIZE_MULTIPLIER);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut phase = Self {
            phase: import_phase,
            sender: Some(sender),
            batch_builder: Vec::with_capacity(Self::BATCH_SIZE),
            workers: Vec::with_capacity(importer.configuration.worker_count()),
            error_state: importer.error_state.clone(),
        };
        for index in 0..importer.configuration.worker_count() {
            let name = format!("importer-{}-{index}", importer.database_name);
            let worker =
                ImportWorker::start(importer, import_phase, receiver.clone(), phase.error_state.clone(), name)?;
            phase.workers.push(worker);
        }
        Ok(phase)
    }

    fn import_concept(&mut self, concept: ImportConcept) -> Result<(), DatabaseImportError> {
        self.batch_builder.push(concept);
        if self.batch_builder.len() >= Self::BATCH_SIZE {
            self.send_batch()?;
        }
        Ok(())
    }

    fn send_batch(&mut self) -> Result<(), DatabaseImportError> {
        if self.batch_builder.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch_builder, Vec::with_capacity(Self::BATCH_SIZE));
        let sent =
            !self.error_state.is_errored() && self.sender.as_ref().is_some_and(|sender| sender.send(batch).is_ok());
        if sent {
            Ok(())
        } else {
            // the workers have stopped: join them to learn why
            self.join();
            Err(self.error_state.error().unwrap_or(DatabaseImportError::ImporterStopped {}))
        }
    }

    fn finish(mut self) -> Result<(), DatabaseImportError> {
        let sent = self.send_batch();
        self.join();
        self.error_state.error().map_or(Ok(()), Err).and(sent)
    }

    fn join(&mut self) {
        self.sender = None; // drop to drain queue
        for worker in self.workers.drain(..) {
            if let Err(payload) = worker.join() {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|message| message.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown reason".to_string());
                self.error_state.stop_with_error(DatabaseImportError::WorkerPanicked { message });
            }
        }
    }

    fn interrupt(&self) {
        self.error_state.stop_with_error(DatabaseImportError::WorkerInterrupted {})
    }
}

impl Drop for DatabaseImporterPhase {
    fn drop(&mut self) {
        if !self.workers.is_empty() {
            self.interrupt();
            self.join();
        }
    }
}

struct ImportWorker {
    importing_database: Arc<dyn ImportingDatabaseHandle>,
    configuration: ImporterConfiguration,
    phase: ImportPhase,
    data_info: Arc<DataInfo>,
    data_transaction: Option<TransactionWrite<WALClient>>,
    total_item_count: Arc<AtomicU64>,
}

impl ImportWorker {
    fn start(
        importer: &DatabaseImporter,
        phase: ImportPhase,
        receiver: Arc<Mutex<Receiver<Vec<ImportConcept>>>>,
        error_state: ErrorState,
        name: String,
    ) -> Result<thread::JoinHandle<()>, DatabaseImportError> {
        let worker = Self {
            importing_database: importer
                .importing_database
                .clone()
                .ok_or(DatabaseImportError::AccessAfterFinalisation {})?,
            phase,
            configuration: importer.configuration.clone(),
            data_info: importer.data_info.clone(),
            data_transaction: None,
            total_item_count: importer.total_item_count.clone(),
        };

        let spawned = thread::Builder::new().name(name).spawn(move || worker.run(receiver, error_state));

        match spawned {
            Ok(handle) => Ok(handle),
            Err(err) => {
                event!(Level::ERROR, "Failed to start an import worker thread: {err}");
                Err(DatabaseImportError::ImporterStopped {})
            }
        }
    }

    fn run(mut self, receiver: Arc<Mutex<Receiver<Vec<ImportConcept>>>>, error_state: ErrorState) {
        if let Err(error) = self.import_batches(&receiver, &error_state) {
            error_state.stop_with_error(error);
        }
    }

    fn import_batches(
        &mut self,
        receiver: &Mutex<Receiver<Vec<ImportConcept>>>,
        error_state: &ErrorState,
    ) -> Result<(), DatabaseImportError> {
        loop {
            let received = receiver.lock().unwrap().recv();
            let Ok(batch) = received else { break };
            for concept in batch {
                if error_state.is_errored() {
                    return Ok(());
                }
                self.apply(concept)?;
            }
        }
        if error_state.is_errored() {
            return Ok(());
        }
        match self.data_transaction.take() {
            Some(transaction) => self.commit_write_transaction(transaction),
            None => Ok(()),
        }
    }

    fn apply(&mut self, concept: ImportConcept) -> Result<(), DatabaseImportError> {
        match concept {
            ImportConcept::Entity(EntityItem { id, label, label_scoped_id, owned_attributes }) => {
                self.import_entity(id, label, label_scoped_id, owned_attributes)
            }
            ImportConcept::Relation(RelationItem {
                id,
                label,
                label_scoped_id,
                owned_attributes,
                related_role_players,
            }) => self.import_relation(id, label, label_scoped_id, owned_attributes, related_role_players),
            ImportConcept::Attribute(AttributeItem { id, label, value }) => self.import_attribute(id, label, value),
            ImportConcept::DeferredOwnership(deferred) => self.import_deferred_ownership(deferred),
            ImportConcept::DeferredLinks(deferred) => self.import_deferred_links(deferred),
        }
    }

    fn import_attribute(&mut self, id: String, label: Label, value: Value<'static>) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let attribute_type = type_manager
                .get_attribute_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownAttributeType { label })?;

            debug_assert!(attribute_type.is_independent(&snapshot, &type_manager).unwrap());
            let attribute = thing_manager
                .create_attribute(&mut snapshot, attribute_type, value.as_reference())
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.data_info.count_attribute();
            if !(self.configuration.skip_recording_of_inline_attributes()
                && attribute.vertex().attribute_id().is_inline())
            {
                self.data_info.record_attribute(id, attribute)?;
            }

            Ok(())
        })
    }

    fn import_entity(
        &mut self,
        id: String,
        label: Label,
        label_scoped_id: Option<u64>,
        owned_attributes: Vec<OwnedAttribute>,
    ) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let entity_type = type_manager
                .get_entity_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownEntityType { label })?;

            let entity = if self.configuration.use_external_label_scoped_ids() {
                let scoped_id = label_scoped_id.ok_or_else(|| DatabaseImportError::LabelScopedIdMissing { id })?;
                thing_manager
                    .unsafe_create_entity_with_id(&mut snapshot, entity_type, ObjectID::new(scoped_id))
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?
            } else {
                let entity = thing_manager
                    .create_entity(&mut snapshot, entity_type)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_entity(id, entity)?;
                entity
            };
            self.data_info.count_entity();

            self.process_owned_attributes(&mut snapshot, &thing_manager, entity.into_object(), owned_attributes)?;
            Ok(())
        })
    }

    fn import_relation(
        &mut self,
        id: String,
        label: Label,
        label_scoped_id: Option<u64>,
        owned_attributes: Vec<OwnedAttribute>,
        role_players: Vec<RolePlayers>,
    ) -> Result<(), DatabaseImportError> {
        if role_players.iter().all(|role_players| role_players.players.is_empty()) {
            return Err(DatabaseImportError::RelationWithoutRolePlayers { id });
        }
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let relation_type = type_manager
                .get_relation_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRelationType { label })?;

            debug_assert!(type_manager.get_is_relation_type_independent(&snapshot, relation_type).unwrap());

            let relation = if self.configuration.use_external_label_scoped_ids() {
                let scoped_id = label_scoped_id.ok_or_else(|| DatabaseImportError::LabelScopedIdMissing { id })?;
                thing_manager
                    .unsafe_create_relation_with_id(&mut snapshot, relation_type, ObjectID::new(scoped_id))
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?
            } else {
                let relation = thing_manager
                    .create_relation(&mut snapshot, relation_type)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_relation(id, relation)?;
                relation
            };
            self.data_info.count_relation();
            self.process_owned_attributes(&mut snapshot, &thing_manager, relation.into_object(), owned_attributes)?;
            self.process_links(&mut snapshot, &type_manager, &thing_manager, relation, role_players)?;
            Ok(())
        })
    }

    fn process_owned_attributes(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        owned_attributes: Vec<OwnedAttribute>,
    ) -> Result<(), DatabaseImportError> {
        for OwnedAttribute { id, attribute_by_value } in owned_attributes {
            // lookup-free path: the attribute's inline id is derived from its value, and the ordering guarantees it exists
            let (value, attribute) = if let Some(AttributeByValue { label, value }) = attribute_by_value {
                if AttributeID::is_inlineable(value.as_reference())
                    && self.configuration.skip_predecessor_phase_existence_checks()
                {
                    let attribute_type = thing_manager
                        .type_manager()
                        .get_attribute_type(snapshot, &label)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .ok_or_else(|| DatabaseImportError::UnknownAttributeType { label: label.clone() })?;

                    let expected_value_type = attribute_type
                        .get_value_type_without_source(snapshot, thing_manager.type_manager())
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .ok_or_else(|| DatabaseImportError::MissingValueType { label: label.clone() })?;
                    let value_value_type = value.value_type();
                    if value_value_type != expected_value_type {
                        return Err(DatabaseImportError::ValueTypeMismatch {
                            label,
                            expected_value_type,
                            actual_value: value.into_owned(),
                            actual_value_type: value_value_type,
                        });
                    }

                    // directly construct Attribute
                    let type_id = attribute_type.vertex().type_id_();
                    let attribute_id = AttributeID::build_inline(value.as_reference());
                    let attribute = Attribute::new(AttributeVertex::new(type_id, attribute_id));

                    // pre-cache the value so validations in set_owns can access it without going to disk
                    let value = Arc::new(value);
                    attribute
                        .cache_value(snapshot, thing_manager, value.clone(), StorageCounters::DISABLED)
                        .map_err(|err| DatabaseImportError::ConceptRead { typedb_source: Box::new(err) })?;
                    (Some(value), Some(attribute))
                } else {
                    (Some(Arc::new(value)), None)
                }
            } else {
                (None, None)
            };

            let attribute = match attribute {
                None => {
                    // unoptimized path: map attribute
                    match self.data_info.get_attribute_by_original_id(snapshot, thing_manager, &id)? {
                        Some(attribute) => {
                            // if we have it, pre-cache the value so validations in set_owns can access it without going to disk
                            if let Some(value) = value {
                                attribute
                                    .cache_value(snapshot, thing_manager, value, StorageCounters::DISABLED)
                                    .map_err(|err| DatabaseImportError::ConceptRead { typedb_source: Box::new(err) })?;
                            }
                            attribute
                        }
                        None => {
                            self.data_info.append_deferred_ownership(DeferredOwnership {
                                attribute_original_id: id,
                                owner_iid: object.iid().into_array(),
                            })?;
                            continue;
                        }
                    }
                }
                Some(attribute) => attribute,
            };
            object
                .set_has_unordered(snapshot, thing_manager, &attribute, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            self.data_info.record_ownership();
        }
        Ok(())
    }

    fn process_links(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation: Relation,
        links: Vec<RolePlayers>,
    ) -> Result<(), DatabaseImportError> {
        let mut resolved_links = Vec::new();
        let mut defer_links = false;
        for RolePlayers { role: label, players } in links {
            let role_type = type_manager
                .get_role_type(snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRoleType { label: label.clone() })?;
            for player in players {
                let resolved = if self.configuration.use_external_label_scoped_ids() {
                    Self::resolve_player_by_label_scoped_id(snapshot, type_manager, player)?
                } else {
                    self.resolve_player_by_original_id(player)?
                };
                defer_links |= !matches!(resolved, ResolvedPlayer::Player(Object::Entity(_)));
                resolved_links.push((role_type, resolved));
            }
        }

        if defer_links {
            let links = resolved_links
                .into_iter()
                .map(|(role_type, resolved)| DeferredLink {
                    role_type_id: role_type.vertex().type_id_().as_u16(),
                    player: match resolved {
                        ResolvedPlayer::Player(object) => DeferredPlayer::ByIid(object.iid().into_array()),
                        ResolvedPlayer::Missing(id) => DeferredPlayer::ByOriginalId(id),
                    },
                })
                .collect();
            self.data_info.append_deferred_links(DeferredLinks { relation_iid: relation.iid().into_array(), links })
        } else {
            for (role_type, resolved) in resolved_links {
                let player = match resolved {
                    ResolvedPlayer::Player(object) => object,
                    ResolvedPlayer::Missing(_) => unreachable!("Non-deferred links must be fully resolved to a player"),
                };
                relation
                    .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_role();
            }
            Ok(())
        }
    }

    fn resolve_player_by_original_id(&self, player: Player) -> Result<ResolvedPlayer, DatabaseImportError> {
        let Player { id, player_by_label_scoped_id: _ } = player;
        Ok(match self.data_info.get_recorded_object(&id)? {
            Some(object) => ResolvedPlayer::Player(object),
            None => ResolvedPlayer::Missing(id),
        })
    }

    fn resolve_player_by_label_scoped_id(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        player: Player,
    ) -> Result<ResolvedPlayer, DatabaseImportError> {
        let Player { id: _, player_by_label_scoped_id } = player;
        let PlayerByLabelScopedId { label, label_scoped_id } =
            player_by_label_scoped_id.ok_or(DatabaseImportError::PlayerLabelScopedIdMissing {})?;
        let player_type = type_manager
            .get_object_type(snapshot, &label)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
            .ok_or_else(|| DatabaseImportError::UnknownPlayerType { label })?;
        let object_id = ObjectID::new(label_scoped_id);
        let object = match player_type {
            ObjectType::Entity(entity_type) => {
                Entity::new(ObjectVertex::build_entity(entity_type.vertex().type_id_(), object_id)).into_object()
            }
            ObjectType::Relation(relation_type) => {
                Relation::new(ObjectVertex::build_relation(relation_type.vertex().type_id_(), object_id)).into_object()
            }
        };
        Ok(ResolvedPlayer::Player(object))
    }

    fn import_deferred_ownership(&mut self, deferred: DeferredOwnership) -> Result<(), DatabaseImportError> {
        let DeferredOwnership { attribute_original_id, owner_iid } = deferred;
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let attribute_opt =
                self.data_info.get_attribute_by_original_id(&snapshot, &thing_manager, &attribute_original_id)?;
            match attribute_opt {
                None => {
                    self.data_info.record_unresolved_ownership();
                    Ok(())
                }
                Some(attribute) => {
                    let owner = Object::new(decode_object_vertex(&owner_iid)?);
                    owner
                        .set_has_unordered(&mut snapshot, &thing_manager, &attribute, StorageCounters::DISABLED)
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    self.data_info.record_ownership();
                    Ok(())
                }
            }
        })
    }

    fn import_deferred_links(&mut self, deferred: DeferredLinks) -> Result<(), DatabaseImportError> {
        let DeferredLinks { relation_iid, links } = deferred;
        let relation = Relation::new(decode_object_vertex(&relation_iid)?);
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            for DeferredLink { role_type_id, player } in links {
                let player = match player {
                    DeferredPlayer::ByOriginalId(id) => match self.data_info.get_recorded_object(&id)? {
                        Some(player) => player,
                        None => {
                            self.data_info.record_unresolved_role_player();
                            continue;
                        }
                    },
                    DeferredPlayer::ByIid(iid) => Object::new(decode_object_vertex(&iid)?),
                };
                let role_type = RoleType::build_from_type_id(TypeID::new(role_type_id));
                relation
                    .add_player(&mut snapshot, &thing_manager, role_type, player, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_role();
            }
            Ok(())
        })
    }

    fn commit_write_transaction(&self, transaction: TransactionWrite<WALClient>) -> Result<(), DatabaseImportError> {
        let map_err = |typedb_source| DatabaseImportError::DataCommitFailed { typedb_source };
        let (_, finalise_result) = transaction.finalise();
        let intent = finalise_result.map_err(|source| map_err(Arc::new(source)))?;
        self.importing_database.commit_data(intent).map_err(map_err)
    }

    fn get_data_transaction(&mut self) -> Result<TransactionWrite<WALClient>, DatabaseImportError> {
        match self.data_transaction.take() {
            Some(transaction) => Ok(transaction),
            None => self
                .importing_database
                .open_write(self.phase.concept_persistence_guarantees(&self.configuration))
                .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source }),
        }
    }

    fn count_item(&self) {
        self.total_item_count.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for ImportWorker {
    fn drop(&mut self) {
        if let Some(data_transaction) = self.data_transaction.take() {
            data_transaction.close();
        }
    }
}

#[derive(Debug)]
struct AttributeEntityRelationConceptPersistenceGuarantees {
    phase: ImportPhase,
    attributes_unique: bool,
}

impl ConceptPersistenceGuarantees for AttributeEntityRelationConceptPersistenceGuarantees {
    fn object_known_to_exist(&self, type_: ObjectType) -> KnownToExist {
        let thing_kind = match type_ {
            ObjectType::Entity(_) => ThingKind::Entity,
            ObjectType::Relation(_) => ThingKind::Relation,
        };
        if self.phase.is_from_previous_phase(thing_kind, ItemOrder::AttributeEntityRelation) {
            KnownToExist::Exists
        } else {
            KnownToExist::Unknown
        }
    }

    fn attribute_known_to_exist(&self, _type: AttributeType) -> KnownToExist {
        if self.phase.is_from_previous_phase(ThingKind::Attribute, ItemOrder::AttributeEntityRelation) {
            KnownToExist::Exists
        } else {
            KnownToExist::Unknown
        }
    }

    fn new_attribute_known_to_exist(&self, _type: AttributeType) -> KnownToExist {
        if self.phase == ImportPhase::Attributes && self.attributes_unique {
            // unique attribute values make every put in the attributes phase a fresh insert
            KnownToExist::NonExistent
        } else {
            KnownToExist::Unknown
        }
    }

    fn new_edge_known_to_exist(&self) -> KnownToExist {
        match self.phase {
            // an Import guarantees that edges are only ever added once
            ImportPhase::Attributes | ImportPhase::Entities | ImportPhase::Relations | ImportPhase::Deferred => {
                KnownToExist::NonExistent
            }
            ImportPhase::Default => KnownToExist::Unknown,
        }
    }
}

typedb_error! {
    pub DatabaseImportError(component = "Database import", prefix = "DBI") {
        TransactionFailed(1, "Import transaction failed.", typedb_source: TransactionError),
        ConceptRead(2, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        ConceptWrite(3, "Error writing concepts.", typedb_source: Box<ConceptWriteError>),
        DataCommitFailed(6, "Import data transaction commit failed.", typedb_source: MigrationOperationError),
        ProvidedSchemaCommitFailed(7, "Imported schema cannot be committed due to errors.", typedb_source: MigrationOperationError),
        PreparationSchemaCommitFailed(8, "Import schema transaction commit failed on preparation. It is a sign of a bug.", typedb_source: MigrationOperationError),
        FinalizationSchemaCommitFailed(9, "Import schema finalization failed: the imported data violates the schema's constraints.", typedb_source: MigrationOperationError),
        SchemaQueryParseFailed(10, "Import schema query parsing failed.", typedb_source: typeql::Error),
        SchemaQueryFailed(11, "Import schema query failed.", typedb_source: Box<QueryError>),
        InvalidSchemaDefineQuery(12, "Import schema query is not a valid define query."),
        DuplicateClientChecksums(13, "Checksums received multiple times. It is a sign of a corrupted file or a client bug."),
        UnknownAttributeType(14, "Cannot process an attribute: attribute type '{label}' does not exist in the schema.", label: Label),
        UnknownEntityType(15, "Cannot process an entity: entity type '{label}' does not exist in the schema.", label: Label),
        UnknownRelationType(16, "Cannot process a relation: relation type '{label}' does not exist in the schema.", label: Label),
        UnknownRoleType(17, "Cannot process a role player: role type '{label}' does not exist in the schema.", label: Label),
        UnknownPlayerType(18, "Cannot process a relation player: the type '{label}' does not exist in the schema.", label: Label),
        NoClientChecksumsOnDone(19, "Cannot verify the imported database as there are no checksums received from the client. It is a sign of a corrupted file or a client bug."),
        InvalidChecksumsOnDone(20, "Invalid imported database with a checksums mismatch: {details}.", details: String),
        IncompleteOwnershipsOnDone(21, "Invalid imported database with {count} unknown owned attributes. It is a sign of a corrupted file or a client bug.", count: usize),
        IncompleteRolesOnDone(22, "Invalid imported database with {count} unknown role players. It is a sign of a corrupted file or a client bug.", count: usize),
        DoubleFinalisation(23, "Error finalizing import for database '{name}': it was already finalized. It is a sign of a corrupted file or a client bug.", name: String),
        AccessAfterFinalisation(24, "Tried to modify the imported database's state after finalization. It is a sign of a client bug."),
        CacheError(25, "Error writing import data.", source: CacheError),
        FinalisationFailed(26, "Import finalisation failed.", typedb_source: MigrationOperationError),
        Interrupted(27, "The import was interrupted by a close request or server shutdown before completion. The import is aborted and can be retried."),
        ItemBeforeSchema(28, "A migration item was received before the schema. The schema is the first message of an import."),
        SchemaAlreadyImported(29, "The schema of this import was already received. An import carries exactly one schema."),
        ItemAfterChecksums(30, "A migration item was received after the checksums. The checksums are the last item of an import."),
        RelationWithoutRolePlayers(31, "Cannot process relation '{id}': it has no role players, and a relation cannot exist without them. It can be a sign of a corrupted file or a client bug.", id: String),
        ImporterStopped(32, "The importer stopped unexpectedly before the import completed."),
        ItemDecode(33, "A migration item could not be decoded.", typedb_source: MigrationOperationError),
        ImportingDatabaseStillShared(34, "Cannot finalise the import while the importing database is still in use. It is a sign of a bug."),
        WorkerInterrupted(35, "Database import worker was interrupted."),
        WorkerPanicked(36, "Database import worker panicked: {message}", message: String),
        ItemOrderViolation(37, "Expected order {order}, but got an item from {current_phase} followed by one from {next_phase}.", order: ItemOrder, current_phase: ImportPhase, next_phase: ImportPhase),
        PlayerLabelScopedIdMissing(38, "A relation player without a label-scoped id was received, but the import guarantees players are referenced by label-scoped ids. It is a sign of a corrupted file or a client bug."),
        UnexpectedPhase(39, "Internal error - mismatched item order {order} with phase {phase}.", order: ItemOrder, phase: ImportPhase),
        LabelScopedIdMissing(41, "Expected entity/relation import message to contain a label-scoped ID, but was missing. Unscoped id: '{id}'", id: String),
        MissingValueType(42, "Cannot process attribute: attribute '{label}' does not have a value type.", label: Label),
        HeaderAfterItems(44, "A header was received after instance items. It is a sign of a corrupted file or a client bug."),
        ValueTypeMismatch(43, "Type {label} requires a value type of {expected_value_type} but got value {actual_value} of value type {actual_value_type}.", label: Label, expected_value_type: ValueType, actual_value: Value<'static>, actual_value_type: ValueType),
    }
}
