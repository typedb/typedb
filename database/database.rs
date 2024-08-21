/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    ffi::OsString,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};
use std::sync::mpsc::{sync_channel, SyncSender};

use concept::{
    error::ConceptWriteError,
    thing::statistics::{Statistics, StatisticsError},
    type_::type_manager::{
        type_cache::{TypeCache, TypeCacheCreateError},
        TypeManager,
    },
};
use concurrency::IntervalRunner;
use durability::wal::{WAL, WALError};
use encoding::{
    EncodingKeyspace,
    error::EncodingError,
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
};
use function::{function_cache::FunctionCache, FunctionError};
use storage::{durability_client::{DurabilityClient, DurabilityClientError, WALClient}, MVCCStorage, recovery::checkpoint::{Checkpoint, CheckpointCreateError, CheckpointLoadError}, sequence_number::SequenceNumber, StorageDeleteError, StorageOpenError, StorageResetError};

use crate::DatabaseOpenError::FunctionCacheInitialise;

#[derive(Debug, Clone)]
pub(super) struct Schema {
    pub(super) thing_statistics: Arc<Statistics>,
    pub(super) type_cache: Arc<TypeCache>,
    pub(super) function_cache: Arc<FunctionCache>,
}

pub struct Database<D> {
    name: String,
    path: PathBuf,
    pub(super) storage: Arc<MVCCStorage<D>>,
    pub(super) definition_key_generator: Arc<DefinitionKeyGenerator>,
    pub(super) type_vertex_generator: Arc<TypeVertexGenerator>,
    pub(super) thing_vertex_generator: Arc<ThingVertexGenerator>,

    pub(super) schema: Arc<RwLock<Schema>>,
    schema_write_transaction_exclusivity: Mutex<(bool, usize, VecDeque<TransactionReservationRequest>)>,

    _statistics_updater: IntervalRunner,
}

enum TransactionReservationRequest {
    Write(SyncSender<()>),
    Schema(SyncSender<()>),
}

impl<D> fmt::Debug for Database<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").field("name", &self.name).field("path", &self.path).finish_non_exhaustive()
    }
}

impl<D> Database<D> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn reserve_write_transaction(&self) {
        let mut guard = self.schema_write_transaction_exclusivity.lock().unwrap();
        let (has_schema_transaction, running_write_transactions, ref mut notify_queue) = *guard;
        if has_schema_transaction || !notify_queue.is_empty() {
            let (sender, receiver) = sync_channel::<()>(0);
            notify_queue.push_back(TransactionReservationRequest::Write(sender));
            drop(guard);
            receiver.recv().unwrap();
        } else {
            guard.1 = running_write_transactions + 1;
            drop(guard);
        }
    }

    pub(super) fn reserve_schema_transaction(&self) {
        let mut guard = self.schema_write_transaction_exclusivity.lock().unwrap();
        let (has_schema_transaction, running_write_transactions, ref mut notify_queue) = *guard;
        if has_schema_transaction || running_write_transactions > 0 || !notify_queue.is_empty() {
            let (sender, receiver) = sync_channel::<()>(0);
            notify_queue.push_back(TransactionReservationRequest::Schema(sender));
            drop(guard);
            receiver.recv().unwrap();
        } else {
            guard.0 = true;
            drop(guard);
        }
    }

    pub(super) fn release_write_transaction(&self) {
        let mut guard = self.schema_write_transaction_exclusivity.lock().unwrap();
        guard.1 = guard.1 - 1;
        if guard.1 == 0 {
            Self::fulfill_reservation_requests(&mut guard)
        }
    }

    pub(super) fn release_schema_transaction(&self) {
        let mut guard = self.schema_write_transaction_exclusivity.lock().unwrap();
        guard.0 = false;
        Self::fulfill_reservation_requests(&mut guard)
    }

    fn fulfill_reservation_requests(guard: &mut MutexGuard<'_, (bool, usize, VecDeque<TransactionReservationRequest>)>) {
        let (has_schema_transaction, running_write_transactions, notify_queue) = &mut **guard;
        if notify_queue.is_empty() {
            return;
        }
        let head = notify_queue.pop_front().unwrap();
        if let TransactionReservationRequest::Schema(notifier) = head {
            // fulfill exactly 1 schema request
            *has_schema_transaction = true;
            notifier.send(()).unwrap();
        } else {
            // fulfill as many write requests as possible
            while let Some(TransactionReservationRequest::Write(notifier)) = notify_queue.pop_front() {
                *running_write_transactions += 1;
                notifier.send(()).unwrap();
            }
        }
    }
}

impl Database<WALClient> {
    pub fn open(path: &Path) -> Result<Database<WALClient>, DatabaseOpenError> {
        use DatabaseOpenError::InvalidUnicodeName;

        let file_name = path.file_name().unwrap();
        let name = file_name.to_str().ok_or_else(|| InvalidUnicodeName { name: file_name.to_owned() })?;

        if path.exists() {
            Self::load(path, name)
        } else {
            Self::create(path, name)
        }
    }

    const STATISTICS_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

    fn create(path: &Path, name: impl AsRef<str>) -> Result<Database<WALClient>, DatabaseOpenError> {
        use DatabaseOpenError::{
            DirectoryCreate, Encoding, FunctionCacheInitialise, StorageOpen, TypeCacheInitialise, WALOpen,
        };

        let name = name.as_ref();

        fs::create_dir(path).map_err(|error| DirectoryCreate { path: path.to_owned(), source: error })?;

        let wal = WAL::create(path).map_err(|error| WALOpen { source: error })?;
        let mut wal_client = WALClient::new(wal);
        wal_client.register_record_type::<Statistics>();

        let storage = Arc::new(
            MVCCStorage::create::<EncodingKeyspace>(name, path, wal_client)
                .map_err(|error| StorageOpen { source: error })?,
        );
        let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator =
            Arc::new(ThingVertexGenerator::load(storage.clone()).map_err(|err| Encoding { source: err })?);
        let thing_statistics = Arc::new(Statistics::new(storage.read_watermark()));

        let type_cache = Arc::new(
            TypeCache::new(storage.clone(), SequenceNumber::MIN)
                .map_err(|error| TypeCacheInitialise { source: error })?,
        );

        let function_cache = Arc::new(
            FunctionCache::new(
                storage.clone(),
                &TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None),
                SequenceNumber::MIN,
            )
                .map_err(|error| FunctionCacheInitialise { source: error })?,
        );

        let schema = Arc::new(RwLock::new(Schema { thing_statistics, type_cache, function_cache }));
        let schema_txn_lock = Arc::new(RwLock::default());

        let update_statistics = make_update_statistics_fn(storage.clone(), schema.clone(), schema_txn_lock.clone());

        Ok(Database::<WALClient> {
            name: name.to_owned(),
            path: path.to_owned(),
            storage,
            definition_key_generator,
            type_vertex_generator,
            thing_vertex_generator,
            schema,
            schema_write_transaction_exclusivity: Mutex::new((false, 0, VecDeque::with_capacity(100))),
            _statistics_updater: IntervalRunner::new(update_statistics, Self::STATISTICS_UPDATE_INTERVAL),
        })
    }

    fn load(path: &Path, name: impl AsRef<str>) -> Result<Database<WALClient>, DatabaseOpenError> {
        use DatabaseOpenError::{
            CheckpointCreate, CheckpointLoad, DurabilityRead, Encoding, StatisticsInitialise, StorageOpen,
            TypeCacheInitialise, WALOpen,
        };

        let wal = WAL::load(path).map_err(|err| WALOpen { source: err })?;
        let wal_last_sequence_number = wal.previous();

        let mut wal_client = WALClient::new(wal);
        wal_client.register_record_type::<Statistics>();

        let checkpoint = Checkpoint::open_latest(path).map_err(|err| CheckpointLoad { source: err })?;
        let storage = Arc::new(
            MVCCStorage::load::<EncodingKeyspace>(&name, path, wal_client, &checkpoint)
                .map_err(|error| StorageOpen { source: error })?,
        );
        let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator =
            Arc::new(ThingVertexGenerator::load(storage.clone()).map_err(|err| Encoding { source: err })?);

        let mut thing_statistics = storage
            .durability()
            .find_last_unsequenced_type::<Statistics>()
            .map_err(|err| DurabilityRead { source: err })?
            .unwrap_or_else(|| Statistics::new(SequenceNumber::MIN));
        thing_statistics.may_synchronise(&storage).map_err(|err| StatisticsInitialise { source: err })?;
        let thing_statistics = Arc::new(thing_statistics);

        let type_cache = Arc::new(
            TypeCache::new(storage.clone(), wal_last_sequence_number)
                .map_err(|error| TypeCacheInitialise { source: error })?,
        );

        let function_cache = Arc::new(
            FunctionCache::new(
                storage.clone(),
                &TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None),
                SequenceNumber::MIN,
            )
                .map_err(|error| FunctionCacheInitialise { source: error })?,
        );

        let schema = Arc::new(RwLock::new(Schema { thing_statistics, type_cache, function_cache }));
        let schema_txn_lock = Arc::new(RwLock::default());

        let update_statistics = make_update_statistics_fn(storage.clone(), schema.clone(), schema_txn_lock.clone());

        let database = Database::<WALClient> {
            name: name.as_ref().to_owned(),
            path: path.to_owned(),
            storage,
            definition_key_generator,
            type_vertex_generator,
            thing_vertex_generator,
            schema,
            schema_write_transaction_exclusivity: Mutex::new((false, 0, VecDeque::with_capacity(100))),
            _statistics_updater: IntervalRunner::new(update_statistics, Self::STATISTICS_UPDATE_INTERVAL),
        };

        let checkpoint_sequence_number =
            checkpoint.as_ref().unwrap().read_sequence_number().map_err(|err| CheckpointLoad { source: err })?;
        if checkpoint_sequence_number < wal_last_sequence_number {
            database.checkpoint().map_err(|err| CheckpointCreate { source: err })?;
        }

        Ok(database)
    }

    fn checkpoint(&self) -> Result<(), DatabaseCheckpointError> {
        let checkpoint =
            Checkpoint::new(&self.path).map_err(|err| DatabaseCheckpointError::CheckpointCreate { source: err })?;

        self.storage
            .checkpoint(&checkpoint)
            .map_err(|err| DatabaseCheckpointError::CheckpointCreate { source: err })?;

        checkpoint.finish().map_err(|err| DatabaseCheckpointError::CheckpointCreate { source: err })?;
        Ok(())
    }

    pub fn delete(self) -> Result<(), DatabaseDeleteError> {
        drop(self._statistics_updater);
        drop(Arc::into_inner(self.schema).expect("Cannot get exclusive ownership of inner of Arc<Schema>."));
        drop(Arc::into_inner(self.type_vertex_generator).expect("Cannot get exclusive ownership of inner of Arc<TypeVertexGenerator>"));
        drop(Arc::into_inner(self.thing_vertex_generator).expect("Cannot get exclusive ownership of inner of Arc<ThingVertexGenerator>"));
        drop(Arc::into_inner(self.definition_key_generator).expect("Cannot get exclusive ownership of inner of Arc<DefinitionKeyGenerator>"));
        Arc::into_inner(self.storage).expect("Cannot get exclusive ownership of inner of Arc<MVCCStorage>.")
            .delete_storage()
            .map_err(|err| DatabaseDeleteError::StorageDelete { source: err })?;
        let path = self.path;
        fs::remove_dir_all(path).map_err(|err| DatabaseDeleteError::DirectoryDelete { source: err })?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), DatabaseResetError> {
        use DatabaseResetError::{
            CorruptionDefinitionKeyGeneratorInUse, CorruptionStorageReset, CorruptionTypeVertexGeneratorInUse,
            TypeVertexGeneratorInUse,
        };

        self.reserve_schema_transaction(); // exclusively lock out other write or schema transactions;
        let mut locked_schema = self.schema.write().unwrap();

        match Arc::get_mut(&mut self.storage) {
            None => return Err(DatabaseResetError::StorageInUse {}),
            Some(storage) => storage.reset().map_err(|err| CorruptionStorageReset { source: err })?,
        }
        match Arc::get_mut(&mut self.definition_key_generator) {
            None => return Err(CorruptionDefinitionKeyGeneratorInUse {}),
            Some(definition_key_generator) => definition_key_generator.reset(),
        }
        match Arc::get_mut(&mut self.type_vertex_generator) {
            None => return Err(CorruptionTypeVertexGeneratorInUse {}),
            Some(type_vertex_generator) => type_vertex_generator.reset(),
        }
        match Arc::get_mut(&mut self.thing_vertex_generator) {
            None => return Err(TypeVertexGeneratorInUse {}),
            Some(thing_vertex_generator) => thing_vertex_generator.reset(),
        }

        let thing_statistics = Arc::get_mut(&mut locked_schema.thing_statistics).unwrap();
        thing_statistics.reset(self.storage.read_watermark());

        self.release_schema_transaction();
        Ok(())
    }
}

fn make_update_statistics_fn(
    storage: Arc<MVCCStorage<WALClient>>,
    schema: Arc<RwLock<Schema>>,
    schema_txn_lock: Arc<RwLock<()>>,
) -> impl Fn() {
    move || {
        let _schema_txn_guard = schema_txn_lock.read().unwrap(); // prevent Schema txns from opening during statistics update
        let mut thing_statistics = (*schema.read().unwrap().thing_statistics).clone();
        thing_statistics.may_synchronise(&storage).ok();
        schema.write().unwrap().thing_statistics = Arc::new(thing_statistics);
    }
}

#[derive(Debug)]
pub enum DatabaseOpenError {
    InvalidUnicodeName { name: OsString },
    CouldNotReadDataDirectory { path: PathBuf, source: io::Error },
    DirectoryCreate { path: PathBuf, source: io::Error },
    StorageOpen { source: StorageOpenError },
    WALOpen { source: WALError },
    DurabilityOpen { source: DurabilityClientError },
    DurabilityRead { source: DurabilityClientError },
    CheckpointLoad { source: CheckpointLoadError },
    CheckpointCreate { source: DatabaseCheckpointError },
    Encoding { source: EncodingError },
    SchemaInitialise { source: ConceptWriteError },
    StatisticsInitialise { source: StatisticsError },
    TypeCacheInitialise { source: TypeCacheCreateError },
    FunctionCacheInitialise { source: FunctionError },
}

impl fmt::Display for DatabaseOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUnicodeName { .. } => None,
            Self::CouldNotReadDataDirectory { source, .. } => Some(source),
            Self::DirectoryCreate { source, .. } => Some(source),
            Self::StorageOpen { source } => Some(source),
            Self::WALOpen { source } => Some(source),
            Self::DurabilityOpen { source } => Some(source),
            Self::DurabilityRead { source } => Some(source),
            Self::CheckpointLoad { source } => Some(source),
            Self::CheckpointCreate { source } => Some(source),
            Self::Encoding { source } => Some(source),
            Self::SchemaInitialise { source } => Some(source),
            Self::StatisticsInitialise { source } => Some(source),
            Self::TypeCacheInitialise { source } => Some(source),
            Self::FunctionCacheInitialise { source } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum DatabaseCheckpointError {
    CheckpointCreate { source: CheckpointCreateError },
}

impl fmt::Display for DatabaseCheckpointError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseCheckpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointCreate { source } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum DatabaseDeleteError {
    StorageDelete { source: StorageDeleteError },
    DirectoryDelete { source: io::Error },
    InUse {},
}

impl fmt::Display for DatabaseDeleteError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseDeleteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StorageDelete { source, .. } => Some(source),
            Self::DirectoryDelete { source } => Some(source),
            Self::InUse { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum DatabaseResetError {
    InUse {},
    StorageInUse {},
    CorruptionStorageReset { source: StorageResetError },
    CorruptionDefinitionKeyGeneratorInUse {},
    CorruptionTypeVertexGeneratorInUse {},
    TypeVertexGeneratorInUse {},
    SchemaInitialise { source: ConceptWriteError },
}

impl fmt::Display for DatabaseResetError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseResetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InUse { .. }
            | Self::StorageInUse { .. }
            | Self::CorruptionDefinitionKeyGeneratorInUse { .. }
            | Self::CorruptionTypeVertexGeneratorInUse { .. }
            | Self::TypeVertexGeneratorInUse { .. } => None,
            Self::CorruptionStorageReset { source, .. } => Some(source),
            Self::SchemaInitialise { source } => Some(source),
        }
    }
}
