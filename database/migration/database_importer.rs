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
        Arc, Mutex, OnceLock,
        atomic::AtomicU64,
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
        thing_manager::{InstanceExistenceOracle, ThingManager},
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
        thing::{ThingVertex, vertex_object::ObjectVertex},
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
    },
    value::{label::Label, value::Value},
};
use error::{TypeDBError, typedb_error};
use query::error::QueryError;
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::StorageCounters};
use serde::{Serialize, de::DeserializeOwned};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{Level, event};
use typeql::{parse_query, query::SchemaQuery};

use crate::{
    migration::{
        Checksums, MigrationOperationError, importing_database_handle::ImportingDatabaseHandle, item::MigrationItem,
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
    objects: Mutex<ObjectsInfo>,
    attributes: Mutex<AttributesInfo>,
    checksums: Checksums,
    expected_checksums: OnceLock<Checksums>,
}

impl DataInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            objects: Mutex::new(ObjectsInfo::new(&cache_directory, database_name)),
            attributes: Mutex::new(AttributesInfo::new(cache_directory, database_name)),
            checksums: Checksums::new(),
            expected_checksums: OnceLock::new(),
        }
    }

    fn record_entity(&self, original_id: String, entity: Entity) -> Result<(), DatabaseImportError> {
        self.checksums.increment_entity_count();
        self.objects.lock().unwrap().instance_id_mapping.record(original_id, entity.into_object())
    }

    fn record_relation(&self, original_id: String, relation: Relation) -> Result<(), DatabaseImportError> {
        self.checksums.increment_relation_count();
        self.objects.lock().unwrap().instance_id_mapping.record(original_id, relation.into_object())
    }

    fn record_attribute(&self, original_id: String, attribute: Attribute) -> Result<(), DatabaseImportError> {
        self.checksums.increment_attribute_count();
        self.attributes.lock().unwrap().instance_id_mapping.record(original_id, attribute)
    }

    fn record_ownership(&self) {
        self.checksums.increment_ownership_count();
    }

    fn record_role(&self) {
        self.checksums.increment_role_count();
    }

    fn get_recorded_object(&self, id: &str) -> Result<Option<Object>, DatabaseImportError> {
        match self.objects.lock().unwrap().instance_id_mapping.get_iid_by_original_id(id)? {
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
        let Some(recorded_iid) = self.attributes.lock().unwrap().instance_id_mapping.get_iid_by_original_id(id)? else {
            return Ok(None);
        };
        thing_manager
            .get_instance::<Attribute>(snapshot, &Bytes::Array(recorded_iid), StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })
    }

    fn append_pending_ownership(&self, pending: PendingOwnership) -> Result<(), DatabaseImportError> {
        self.attributes.lock().unwrap().pending_ownerships.append(pending)
    }

    fn append_pending_links(&self, pending: PendingLinks) -> Result<(), DatabaseImportError> {
        self.objects.lock().unwrap().pending_links.append(pending)
    }

    fn take_pending_ownerships(&self) -> Option<SpilloverCache<PendingOwnership>> {
        self.attributes.lock().unwrap().pending_ownerships.take()
    }

    fn take_pending_links(&self) -> Option<SpilloverCache<PendingLinks>> {
        self.objects.lock().unwrap().pending_links.take()
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
struct PendingOwnership {
    attribute_original_id: String,
    owner_iid: IID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingLinks {
    relation_iid: IID,
    links: Vec<PendingLink>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingLink {
    role_type_id: TypeIDUInt,
    player_original_id: String,
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
            .expect("Pending records are only appended before the import stream is done")
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
    pub pending_links: SpilloverLog<PendingLinks>,
}

impl ObjectsInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            pending_links: SpilloverLog::new(cache_directory, database_name),
        }
    }
}

#[derive(Debug)]
struct AttributesInfo {
    pub instance_id_mapping: InstanceIDMapping<Attribute>,
    pub pending_ownerships: SpilloverLog<PendingOwnership>,
}

impl AttributesInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            pending_ownerships: SpilloverLog::new(cache_directory, database_name),
        }
    }
}

type ImportItems = Box<dyn Iterator<Item = Result<MigrationItem, MigrationOperationError>> + Send>;

enum ImportMessage {
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

    phase: Option<DatabaseImporterPhase>,
    batch: Vec<MigrationItem>,
    data_transaction: Option<TransactionWrite<WALClient>>, // for the finalize phase
}

impl Debug for DatabaseImporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseImporter").field("database_name", &self.database_name).finish_non_exhaustive()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ItemOrder {
    Unspecified,
    AttributeEntityRelation,
}

impl Display for ItemOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Copy, Clone)]
pub struct ImporterConfiguration {
    max_writes_per_commit: usize,
    buffer_size: usize,
    item_order: ItemOrder,

    map_object_ids: bool,
    map_inline_attribute_ids: bool,
}

impl Default for ImporterConfiguration {
    fn default() -> Self {
        Self {
            max_writes_per_commit: Self::MAX_WRITES_PER_COMMIT,
            buffer_size: 2, // TODO,
            item_order: ItemOrder::Unspecified,

            map_object_ids: true,
            map_inline_attribute_ids: true,
        }
    }
}

impl ImporterConfiguration {
    const MAX_WRITES_PER_COMMIT: usize = 10_000;

    pub fn with_buffer_size(self, buffer_size: usize) -> Self {
        Self { buffer_size, ..self }
    }

    pub fn with_max_writes_per_commit(self, max_writes_per_commit: usize) -> Self {
        Self { max_writes_per_commit, ..self }
    }

    fn with_item_order(self, item_order: ItemOrder) -> Self {
        Self { item_order, ..self }
    }

    fn worker_count(&self) -> usize {
        match self.item_order {
            ItemOrder::Unspecified => 1,
            ItemOrder::AttributeEntityRelation => 8, // TODO use cpu count
        }
    }

    fn skip_predecessor_phase_existence_checks(&self) -> bool {
        matches!(self.item_order, ItemOrder::AttributeEntityRelation)
    }
}

impl DatabaseImporter {
    const DRAIN_CHUNK_SIZE: usize = 10_000;
    const BATCH_SIZE: usize = 1_000;

    pub fn start(
        importing_database: Arc<dyn ImportingDatabaseHandle>,
        schema: String,
        importer_configuration: ImporterConfiguration,
        scratch_directory: PathBuf,
        task_spawner: &TokioTaskSpawner,
    ) -> DatabaseImporterHandle {
        let (sender, receiver) = mpsc::channel(importer_configuration.buffer_size);
        let error_state = ErrorState::default();
        let database_name = importing_database.database_name().to_owned();
        let data_info = Arc::new(DataInfo::new(&scratch_directory, &database_name));
        let mut importer = Self {
            importing_database: Some(importing_database),
            database_name,
            configuration: importer_configuration,
            schema_info: SchemaInfo::new(),
            data_info,
            total_item_count: Arc::new(AtomicU64::new(0)),
            error_state: error_state.clone(),
            phase: None,
            batch: Vec::with_capacity(Self::BATCH_SIZE),
            data_transaction: None,
        };
        let join_handle = task_spawner.spawn_blocking(move || {
            importer.import_schema(schema)?;
            importer.listen(receiver)
        });
        DatabaseImporterHandle { message_sender: Some(sender), error_state, join_handle: Some(join_handle) }
    }

    fn listen(mut self, mut receiver: mpsc::Receiver<ImportMessage>) -> ImportResult {
        loop {
            self.check_stopped()?;
            match receiver.blocking_recv() {
                // The handle closed the channel: report an error already recorded, e.g. by a worker, over the interrupt
                None => return Err(self.error_state.error().unwrap_or(DatabaseImportError::Interrupted {})),
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
            MigrationItem::Header { typedb_version, original_database } => {
                event!(
                    Level::DEBUG,
                    "Importing '{original_database}' from TypeDB {typedb_version} to '{}'.",
                    self.database_name
                );
                Ok(())
            }
            item
            @ (MigrationItem::Entity { .. } | MigrationItem::Relation { .. } | MigrationItem::Attribute { .. }) => {
                self.dispatch(item)
            }
        }
    }

    fn dispatch(&mut self, item: MigrationItem) -> Result<(), DatabaseImportError> {
        let thing_kind = item.thing_kind().ok_or_else(|| DatabaseImportError::ExpectedInstanceItem {})?;
        let phase = ImportPhase::phase_for(thing_kind, self.configuration.item_order);
        match self.phase.as_ref().map(|current| current.phase) {
            None => self.phase = Some(DatabaseImporterPhase::start(self, phase)?),
            Some(current_phase) => {
                if current_phase != phase {
                    current_phase.validate_transition(phase, self.configuration.item_order)?;
                    self.finish_phase()?;
                    self.phase = Some(DatabaseImporterPhase::start(self, phase)?);
                }
            }
        }
        self.batch.push(item);
        if self.batch.len() >= Self::BATCH_SIZE {
            self.send_batch()?;
        }
        Ok(())
    }

    fn send_batch(&mut self) -> Result<(), DatabaseImportError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(Self::BATCH_SIZE));
        let phase = self.phase.as_ref().expect("Batches are only built within a phase");
        if phase.send(batch) {
            Ok(())
        } else {
            self.finish_phase()?;
            Err(DatabaseImportError::ImporterStopped {})
        }
    }

    fn finish_phase(&mut self) -> Result<(), DatabaseImportError> {
        if self.phase.is_none() {
            return Ok(());
        }
        let sent = self.send_batch();
        let finished = self.phase.take().map_or(Ok(()), DatabaseImporterPhase::finish);
        finished.and(sent)
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
        self.drain_pending_ownerships()?;
        self.check_stopped()?;

        // TODO: this could itself encompass a multithreaded phase
        self.drain_pending_links()?;

        if let Some(data_transaction) = self.data_transaction.take() {
            self.commit_write_transaction(data_transaction)?;
        }

        self.check_stopped()?;
        self.validate_imported_data()?;
        self.check_stopped()?;
        self.restore_relaxed_schema()?;
        self.check_stopped()?;

        let Some(importing_database) = self.importing_database.take() else {
            return Err(DatabaseImportError::DoubleFinalisation { name: self.database_name().to_string() });
        };

        match importing_database.into_unique() {
            Ok(importing_database) => importing_database
                .finalise()
                .map_err(|typedb_source| DatabaseImportError::FinalisationFailed { typedb_source }),
            Err(_) => Err(DatabaseImportError::ImportingDatabaseStillShared {}),
        }
    }

    fn drain_pending_ownerships(&mut self) -> Result<(), DatabaseImportError> {
        let records = self.data_info.take_pending_ownerships();
        match self.drain_pending(records, "ownerships", Self::import_pending_ownership)? {
            0 => Ok(()),
            unresolved => Err(DatabaseImportError::IncompleteOwnershipsOnDone { count: unresolved }),
        }
    }

    fn drain_pending_links(&mut self) -> Result<(), DatabaseImportError> {
        let records = self.data_info.take_pending_links();
        match self.drain_pending(records, "relations' pending links", Self::import_pending_links)? {
            0 => Ok(()),
            unresolved => Err(DatabaseImportError::IncompleteRolesOnDone { count: unresolved }),
        }
    }

    fn drain_pending<T>(
        &mut self,
        records: Option<SpilloverCache<T>>,
        concept_name: &str,
        import_one: impl Fn(&mut Self, T, &mut usize) -> Result<(), DatabaseImportError>,
    ) -> Result<usize, DatabaseImportError>
    where
        T: Serialize + DeserializeOwned + Clone,
    {
        let Some(records) = records else { return Ok(0) };
        let (mut written, mut unresolved) = (0u64, 0);
        for chunk in records.into_chunks(Self::DRAIN_CHUNK_SIZE) {
            self.check_stopped()?;
            for (_key, pending) in chunk.map_err(|source| DatabaseImportError::CacheError { source })? {
                import_one(self, pending, &mut unresolved)?;
                written += 1;
            }
        }
        if written > 0 {
            event!(
                Level::DEBUG,
                "Wrote {written} {concept_name} deferred until their referenced instances were imported."
            );
        }
        Ok(unresolved)
    }

    fn import_pending_ownership(
        &mut self,
        pending: PendingOwnership,
        unknown_attributes: &mut usize,
    ) -> Result<(), DatabaseImportError> {
        let PendingOwnership { attribute_original_id, owner_iid } = pending;
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let attribute_opt =
                self.data_info.get_attribute_by_original_id(&snapshot, &thing_manager, &attribute_original_id)?;
            match attribute_opt {
                None => {
                    *unknown_attributes += 1;
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

    fn import_pending_links(
        &mut self,
        pending: PendingLinks,
        unknown_players: &mut usize,
    ) -> Result<(), DatabaseImportError> {
        let PendingLinks { relation_iid, links } = pending;
        let relation = Relation::new(decode_object_vertex(&relation_iid)?);
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            for PendingLink { role_type_id, player_original_id } in links {
                let Some(player) = self.data_info.get_recorded_object(&player_original_id)? else {
                    *unknown_players += 1;
                    continue;
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

    fn database_name(&self) -> &str {
        &self.database_name
    }

    fn total_item_count(&self) -> u64 {
        self.total_item_count.load(core::sync::atomic::Ordering::Relaxed) // TODO: right level?
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

    fn count_item(&mut self) {
        self.total_item_count.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    // TODO: this is only used for the pending phase right now, but isn't architected to show this
    fn get_data_transaction(&mut self) -> Result<TransactionWrite<WALClient>, DatabaseImportError> {
        match self.data_transaction.take() {
            Some(transaction) => Ok(transaction),
            None => self
                .importing_database()?
                .open_write(ImportPhase::Pending.existence_oracle(self.configuration.item_order))
                .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source }),
        }
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

    fn commit_write_transaction(&self, transaction: TransactionWrite<WALClient>) -> Result<(), DatabaseImportError> {
        let map_err = |typedb_source| DatabaseImportError::DataCommitFailed { typedb_source };
        let (_, finalise_result) = transaction.finalise();
        let intent = finalise_result.map_err(|source| map_err(Arc::new(source)))?;
        self.importing_database()?.commit_data(intent).map_err(map_err)
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

impl Drop for DatabaseImporter {
    fn drop(&mut self) {
        if let Some(data_transaction) = self.data_transaction.take() {
            assert!(
                self.importing_database.is_some(),
                "Unexpected open import transaction while the import is still not done"
            );
            data_transaction.close();
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ImportPhase {
    Default,
    Attributes,
    Entities,
    Relations,
    Pending,
}

impl ImportPhase {
    fn existence_oracle(&self, item_order: ItemOrder) -> Option<Box<dyn InstanceExistenceOracle>> {
        match item_order {
            ItemOrder::Unspecified => None,
            ItemOrder::AttributeEntityRelation => {
                Some(Box::new(AttributeEntityRelationExistenceOracle { phase: *self }))
            }
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
                (Self::Default, Self::Default | Self::Pending) => Ok(()),
                (_, _) => Err(DatabaseImportError::UnexpectedPhase { order: item_order, phase: next_phase }),
            },
            ItemOrder::AttributeEntityRelation => match (self, next_phase) {
                (Self::Attributes, Self::Attributes | Self::Entities | Self::Relations | Self::Pending) => Ok(()),
                (Self::Attributes, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Entities, Self::Entities | Self::Relations | Self::Pending) => Ok(()),
                (Self::Entities, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Relations, Self::Relations | Self::Pending) => Ok(()),
                (Self::Relations, _) => {
                    Err(DatabaseImportError::ItemOrderViolation { order: item_order, current_phase: *self, next_phase })
                }

                (Self::Pending, Self::Pending) => Ok(()),
                (Self::Pending, _) => {
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

                (Self::Pending, Self::Entities | Self::Attributes | Self::Relations) => true,
                (Self::Pending, _) => false,

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
    sender: Option<SyncSender<Vec<MigrationItem>>>,
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

    fn start(importer: &DatabaseImporter, import_phase: ImportPhase) -> Result<Self, DatabaseImportError> {
        let (sender, receiver) = sync_channel(importer.configuration.worker_count() * Self::QUEUE_SIZE_MULTIPLIER);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut phase = Self {
            phase: import_phase,
            sender: Some(sender),
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

    fn send(&self, batch: Vec<MigrationItem>) -> bool {
        !self.error_state.is_errored() && self.sender.as_ref().is_some_and(|sender| sender.send(batch).is_ok())
    }

    fn finish(mut self) -> Result<(), DatabaseImportError> {
        self.join();
        self.error_state.error().map_or(Ok(()), Err)
    }

    fn join(&mut self) {
        self.sender = None; // drop to drain queue
        for worker in self.workers.drain(..) {
            // A panicked worker. The server's panic hook exits the process first, so this only applies without it
            let name = worker.thread().name().unwrap_or("import worker").to_owned();
            if let Err(payload) = worker.join() {
                let message = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("unknown reason");
                event!(Level::ERROR, "Import worker '{name}' panicked: {message}");
                self.error_state.stop_with_error(DatabaseImportError::WorkerStopped {});
            }
        }
    }

    fn interrupt(&self) {
        self.error_state.stop_with_error(DatabaseImportError::WorkerInterrupted {})
    }
}

impl Drop for DatabaseImporterPhase {
    fn drop(&mut self) {
        // Unfinished phase, e.g. the import failed or was interrupted: abandon the queued batches
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
        receiver: Arc<Mutex<Receiver<Vec<MigrationItem>>>>,
        error_state: ErrorState,
        name: String,
    ) -> Result<thread::JoinHandle<()>, DatabaseImportError> {
        let worker = Self {
            importing_database: importer
                .importing_database
                .clone()
                .ok_or(DatabaseImportError::AccessAfterFinalisation {})?,
            phase,
            configuration: importer.configuration,
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

    fn run(mut self, receiver: Arc<Mutex<Receiver<Vec<MigrationItem>>>>, error_state: ErrorState) {
        if let Err(error) = self.import_batches(&receiver, &error_state) {
            error_state.stop_with_error(error);
        }
    }

    fn import_batches(
        &mut self,
        receiver: &Mutex<Receiver<Vec<MigrationItem>>>,
        error_state: &ErrorState,
    ) -> Result<(), DatabaseImportError> {
        loop {
            // The lock is released before importing, so the other workers can receive batches meanwhile
            let received = receiver.lock().unwrap().recv();
            let Ok(batch) = received else { break };
            for item in batch {
                // Set by an interrupt from the handle, or by a failed worker
                if error_state.is_errored() {
                    return Ok(());
                }
                self.apply(item)?;
            }
        }
        // Stopped while waiting for a batch: the open transaction is dropped without committing
        if error_state.is_errored() {
            return Ok(());
        }
        // All batches of the phase are imported: commit so the next phase can see them
        match self.data_transaction.take() {
            Some(transaction) => self.commit_write_transaction(transaction),
            None => Ok(()),
        }
    }

    fn apply(&mut self, item: MigrationItem) -> Result<(), DatabaseImportError> {
        match item {
            MigrationItem::Entity { id, label, owned_attributes } => self.import_entity(id, label, owned_attributes),
            MigrationItem::Relation { id, label, owned_attributes, related_role_players } => {
                self.import_relation(id, label, owned_attributes, related_role_players)
            }
            MigrationItem::Attribute { id, label, value } => self.import_attribute(id, label, value),
            MigrationItem::Header { .. } | MigrationItem::Checksums(_) => {
                unreachable!("Only instance items are dispatched to import workers")
            }
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
                .create_attribute(&mut snapshot, attribute_type, value)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.data_info.record_attribute(id, attribute)
        })
    }

    fn import_entity(
        &mut self,
        id: String,
        label: Label,
        owned_attributes: Vec<String>,
    ) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let entity_type = type_manager
                .get_entity_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownEntityType { label })?;

            let entity = thing_manager
                .create_entity(&mut snapshot, entity_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.process_owned_attributes(&mut snapshot, &thing_manager, entity.into_object(), owned_attributes)?;

            self.data_info.record_entity(id, entity)
        })
    }

    fn import_relation(
        &mut self,
        id: String,
        label: Label,
        owned_attributes: Vec<String>,
        links: Vec<(Label, Vec<String>)>,
    ) -> Result<(), DatabaseImportError> {
        if links.iter().all(|(_, players)| players.is_empty()) {
            return Err(DatabaseImportError::RelationWithoutRolePlayers { id });
        }
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let relation_type = type_manager
                .get_relation_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRelationType { label })?;

            debug_assert!(type_manager.get_is_relation_type_independent(&snapshot, relation_type).unwrap());

            let relation = thing_manager
                .create_relation(&mut snapshot, relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.process_owned_attributes(&mut snapshot, &thing_manager, relation.into_object(), owned_attributes)?;
            self.process_links(&mut snapshot, &type_manager, &thing_manager, relation, links)?;

            self.data_info.record_relation(id, relation)
        })
    }

    fn process_owned_attributes(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        owned_attribute_ids: Vec<String>,
    ) -> Result<(), DatabaseImportError> {
        for id in owned_attribute_ids {
            match self.data_info.get_attribute_by_original_id(snapshot, thing_manager, &id)? {
                Some(attribute) => {
                    object
                        .set_has_unordered(snapshot, thing_manager, &attribute, StorageCounters::DISABLED)
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    self.data_info.record_ownership();
                }
                None => {
                    self.data_info.append_pending_ownership(PendingOwnership {
                        attribute_original_id: id,
                        owner_iid: object.iid().into_array(),
                    })?;
                }
            }
        }
        Ok(())
    }

    fn process_links(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation: Relation,
        links: Vec<(Label, Vec<String>)>,
    ) -> Result<(), DatabaseImportError> {
        let mut resolved_links = Vec::new();
        let mut defer_links = false;
        for (label, player_ids) in links {
            let role_type = type_manager
                .get_role_type(snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRoleType { label: label.clone() })?;
            for id in player_ids {
                let player = self.data_info.get_recorded_object(&id)?;
                defer_links |= !matches!(player, Some(Object::Entity(_)));
                resolved_links.push((role_type, id, player));
            }
        }

        if defer_links {
            let links = resolved_links
                .into_iter()
                .map(|(role_type, id, _)| PendingLink {
                    role_type_id: role_type.vertex().type_id_().as_u16(),
                    player_original_id: id,
                })
                .collect();
            return self
                .data_info
                .append_pending_links(PendingLinks { relation_iid: relation.iid().into_array(), links });
        }
        for (role_type, _, player) in resolved_links {
            let player = player.expect("Every player is a recorded entity unless the links are deferred");
            relation
                .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            self.data_info.record_role();
        }
        Ok(())
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
                .open_write(self.phase.existence_oracle(self.configuration.item_order))
                .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source }),
        }
    }

    fn count_item(&self) {
        self.total_item_count.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for ImportWorker {
    fn drop(&mut self) {
        // Left open only if the worker stopped early: its writes are abandoned with the import
        if let Some(data_transaction) = self.data_transaction.take() {
            data_transaction.close();
        }
    }
}

struct AttributeEntityRelationExistenceOracle {
    phase: ImportPhase,
}

impl Debug for AttributeEntityRelationExistenceOracle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AttributeEntityRelationExistenceOracle[phase={}]", self.phase)
    }
}

impl InstanceExistenceOracle for AttributeEntityRelationExistenceOracle {
    fn known_persisted_object(&self, object: &Object) -> bool {
        self.phase.is_from_previous_phase(object.thing_kind(), ItemOrder::AttributeEntityRelation)
    }

    fn known_persisted_attribute(&self, attribute: &Attribute) -> bool {
        self.phase.is_from_previous_phase(attribute.thing_kind(), ItemOrder::AttributeEntityRelation)
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
        NoClientChecksumsOnDone(18, "Cannot verify the imported database as there are no checksums received from the client. It is a sign of a corrupted file or a client bug."),
        InvalidChecksumsOnDone(19, "Invalid imported database with a checksums mismatch: {details}.", details: String),
        IncompleteOwnershipsOnDone(20, "Invalid imported database with {count} unknown owned attributes. It is a sign of a corrupted file or a client bug.", count: usize),
        IncompleteRolesOnDone(21, "Invalid imported database with {count} unknown role players. It is a sign of a corrupted file or a client bug.", count: usize),
        DoubleFinalisation(22, "Error finalizing import for database '{name}': it was already finalized. It is a sign of a corrupted file or a client bug.", name: String),
        AccessAfterFinalisation(24, "Tried to modify the imported database's state after finalization. It is a sign of a client bug."),
        CacheError(25, "Error writing import data.", source: CacheError),
        FinalisationFailed(26, "Import finalisation failed.", typedb_source: MigrationOperationError),
        Interrupted(27, "The import was interrupted by a close request or server shutdown before completion. The import is aborted and can be retried."),
        ItemBeforeSchema(28, "A migration item was received before the schema. The schema is the first item of an import."),
        SchemaAlreadyImported(29, "The schema of this import was already received. An import carries exactly one schema."),
        ItemAfterChecksums(30, "A migration item was received after the checksums. The checksums are the last item of an import."),
        RelationWithoutRolePlayers(31, "Cannot process relation '{id}': it has no role players, and a relation cannot exist without them. It can be a sign of a corrupted file or a client bug.", id: String),
        ImporterStopped(32, "The importer stopped unexpectedly before the import completed."),
        ItemDecode(33, "A migration item could not be decoded.", typedb_source: MigrationOperationError),
        ImportingDatabaseStillShared(34, "Cannot finalise the import while the importing database is still in use. It is a sign of a bug."),
        WorkerInterrupted(35, "Database import worker was interrupted."),
        WorkerStopped(36, "Database import worker stopped unexpectedly before the import completed."),
        ItemOrderViolation(37, "Expected order {order}, but got an item from {current_phase} followed by one from {next_phase}.", order: ItemOrder, current_phase: ImportPhase, next_phase: ImportPhase),
        UnexpectedPhase(38, "Internal error - mismatched item order {order} with phase {phase}.", order: ItemOrder, phase: ImportPhase),
        ExpectedInstanceItem(39, "Internal error - expected an item message containing an instance."),
    }
}
