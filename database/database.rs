/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::VecDeque,
    ffi::OsString,
    fmt, fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc, Mutex, MutexGuard, RwLock, TryLockError,
    },
    time::{Duration, Instant},
};

use concept::{
    thing::statistics::{Statistics, StatisticsError},
    type_::type_manager::{
        type_cache::{TypeCache, TypeCacheCreateError},
        TypeManager,
    },
};
use concurrency::IntervalRunner;
use diagnostics::metrics::{DataLoadMetrics, DatabaseMetrics, SchemaLoadMetrics};
use durability::{wal::WAL, DurabilitySequenceNumber, DurabilityServiceError};
use encoding::{
    error::EncodingError,
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    EncodingKeyspace,
};
use error::typedb_error;
use function::{function_cache::FunctionCache, FunctionError};
use query::query_cache::QueryCache;
use resource::{
    constants::database::{CHECKPOINT_INTERVAL, STATISTICS_UPDATE_INTERVAL},
    profile::CommitProfile,
};
use storage::{
    durability_client::{DurabilityClient, DurabilityClientError, WALClient},
    record::CommitRecord,
    recovery::checkpoint::{Checkpoint, CheckpointCreateError, CheckpointLoadError},
    sequence_number::SequenceNumber,
    snapshot::{snapshot_id::SnapshotId, CommittableSnapshot, SchemaSnapshot, WriteSnapshot},
    MVCCStorage, StorageDeleteError, StorageOpenError, StorageResetError,
};
use tracing::{event, Level};

use crate::{
    transaction::{
        DataCommitError, SchemaCommitError,
        SchemaCommitError::{SnapshotError, TypeCacheUpdateError},
        TransactionError,
    },
    DatabaseOpenError::FunctionCacheInitialise,
    DatabaseResetError::{
        CorruptionPartialResetKeyGeneratorInUse, CorruptionPartialResetThingVertexGeneratorInUse,
        CorruptionPartialResetTypeVertexGeneratorInUse,
    },
};

#[derive(Debug, Clone)]
pub(super) struct Schema {
    pub(super) thing_statistics: Arc<Statistics>,
    pub(super) type_cache: Arc<TypeCache>,
    pub(super) function_cache: Arc<FunctionCache>,
}

type SchemaWriteTransactionState = (bool, usize, VecDeque<TransactionReservationRequest>);

enum TransactionReservationRequest {
    Write(SyncSender<()>),
    Schema(SyncSender<()>),
}

pub struct Database<D> {
    name: String,
    pub(super) path: PathBuf,
    pub(super) storage: Arc<MVCCStorage<D>>,
    pub(super) definition_key_generator: Arc<DefinitionKeyGenerator>,
    pub(super) type_vertex_generator: Arc<TypeVertexGenerator>,
    pub(super) thing_vertex_generator: Arc<ThingVertexGenerator>,

    pub(super) schema: Arc<RwLock<Schema>>,
    pub(super) query_cache: Arc<QueryCache>,
    schema_write_transaction_exclusivity: Mutex<SchemaWriteTransactionState>,
    _statistics_updater: IntervalRunner,
    _checkpointer: IntervalRunner,
}

impl<D> fmt::Debug for Database<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").field("name", &self.name).field("path", &self.path).finish_non_exhaustive()
    }
}

impl<D> Database<D> {
    const TRY_LOCK_SLEEP_INTERVAL: Duration = Duration::from_millis(10);

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn reserve_write_transaction(&self, timeout_millis: u64) -> Result<(), TransactionError> {
        let (mut guard, timeout_left) =
            self.try_acquire_schema_write_transaction_lock(Duration::from_millis(timeout_millis))?;
        let (has_schema_transaction, running_write_transactions, ref mut notify_queue) = *guard;

        if has_schema_transaction || !notify_queue.is_empty() {
            let (sender, receiver) = sync_channel::<()>(0);
            notify_queue.push_back(TransactionReservationRequest::Write(sender));
            drop(guard);
            receiver.recv_timeout(timeout_left).map_err(|source| TransactionError::Timeout { source })?;
        } else {
            guard.1 = running_write_transactions + 1;
            drop(guard);
        }
        Ok(())
    }

    pub(super) fn reserve_schema_transaction(&self, timeout_millis: u64) -> Result<(), TransactionError> {
        let (mut guard, timeout_left) =
            self.try_acquire_schema_write_transaction_lock(Duration::from_millis(timeout_millis))?;
        let (has_schema_transaction, running_write_transactions, ref mut notify_queue) = *guard;

        if has_schema_transaction || running_write_transactions > 0 || !notify_queue.is_empty() {
            let (sender, receiver) = sync_channel::<()>(0);
            notify_queue.push_back(TransactionReservationRequest::Schema(sender));
            drop(guard);
            receiver.recv_timeout(timeout_left).map_err(|source| TransactionError::Timeout { source })?;
        } else {
            guard.0 = true;
            drop(guard);
        }
        Ok(())
    }

    pub(super) fn release_write_transaction(&self) {
        let mut guard = self
            .schema_write_transaction_exclusivity
            .lock()
            .expect("The exclusive access should already be acquired in `reserve`");
        guard.1 -= 1;
        if guard.1 == 0 {
            Self::fulfill_reservation_requests(&mut guard)
        }
    }

    pub(super) fn release_schema_transaction(&self) {
        let mut guard = self
            .schema_write_transaction_exclusivity
            .lock()
            .expect("The exclusive access should already be acquired in `reserve`");
        guard.0 = false;
        Self::fulfill_reservation_requests(&mut guard)
    }

    fn try_acquire_schema_write_transaction_lock(
        &self,
        timeout: Duration,
    ) -> Result<(MutexGuard<'_, SchemaWriteTransactionState>, Duration), TransactionError> {
        let start_time = Instant::now();
        let guard = loop {
            match self.schema_write_transaction_exclusivity.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => {
                    if start_time.elapsed() >= timeout {
                        return Err(TransactionError::WriteExclusivityTimeout {});
                    }
                    std::thread::sleep(Self::TRY_LOCK_SLEEP_INTERVAL);
                }
                Err(TryLockError::Poisoned(err)) => panic!(
                    "Encountered a poisoned lock while trying to acquire exclusive schema write transaction access: {}",
                    err
                ),
            }
        };

        let elapsed = start_time.elapsed();
        let remaining_timeout = if timeout < elapsed { Duration::from_millis(0) } else { timeout - elapsed };

        Ok((guard, remaining_timeout))
    }

    fn fulfill_reservation_requests(
        guard: &mut MutexGuard<'_, (bool, usize, VecDeque<TransactionReservationRequest>)>,
    ) {
        let (has_schema_transaction, running_write_transactions, notify_queue) = &mut **guard;

        loop {
            let (next_schema, next_write) = match notify_queue.front() {
                Some(TransactionReservationRequest::Schema(_)) => (true, false),
                Some(TransactionReservationRequest::Write(_)) => (false, true),
                None => (false, false),
            };

            if next_schema {
                if *running_write_transactions > 0 {
                    // wait for the write transactions to finish, leave the request in the queue
                    break;
                }
                let TransactionReservationRequest::Schema(notifier) =
                    notify_queue.pop_front().expect("Expected the next schema request")
                else {
                    panic!("Expected the next schema request: the queue cannot be changed")
                };
                if notifier.send(()).is_ok() {
                    // fulfill exactly 1 awaiting schema request
                    *has_schema_transaction = true;
                    break;
                }
            } else if next_write {
                let TransactionReservationRequest::Write(notifier) =
                    notify_queue.pop_front().expect("Expected the next write request")
                else {
                    panic!("Expected the next write request: the queue cannot be changed")
                };
                if notifier.send(()).is_ok() {
                    // fulfill as many write requests as possible
                    *running_write_transactions += 1;
                }
            } else {
                break;
            }
        }
    }
}

impl<D: DurabilityClient> Database<D> {
    pub fn data_commit_with_commit_record(
        &self,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), DataCommitError> {
        let snapshot = WriteSnapshot::new_with_commit_record(self.storage.clone(), commit_record);
        self.data_commit_with_snapshot(snapshot, commit_profile)
    }

    pub fn data_commit_with_snapshot(
        &self,
        snapshot: WriteSnapshot<D>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), DataCommitError> {
        let commit_record_opt = snapshot
            .finalise(commit_profile)
            .map_err(|error| DataCommitError::SnapshotError { typedb_source: error })?;

        if let Some(commit_record) = commit_record_opt {
            let commit_result = self.storage.commit(commit_record, commit_profile);
            match commit_result {
                Ok(_) => Ok(()),
                Err(error) => {
                    let error = DataCommitError::SnapshotError {
                        typedb_source: storage::snapshot::SnapshotError::Commit { typedb_source: error },
                    };
                    Err(error)
                }
            }
        } else {
            Ok(())
        }
    }

    pub fn schema_commit_with_commit_record(
        &self,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), SchemaCommitError> {
        let snapshot = SchemaSnapshot::new_with_commit_record(self.storage.clone(), commit_record);
        self.schema_commit_with_snapshot(snapshot, commit_profile)
    }

    pub fn schema_commit_with_snapshot(
        &self,
        snapshot: SchemaSnapshot<D>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), SchemaCommitError> {
        // Schema commits must wait for all other data operations to finish. No new read or write
        // transaction may open until the commit completes.
        let mut schema_commit_guard = self.schema.write().unwrap();
        let mut schema = (*schema_commit_guard).clone();

        let commit_record_opt = snapshot
            .finalise(commit_profile)
            .map_err(|error| SchemaCommitError::SnapshotError { typedb_source: error })?;

        if let Some(commit_record) = commit_record_opt {
            let sequence_number = match self.storage.commit(commit_record, commit_profile) {
                Ok(sequence_number) => Some(sequence_number),
                Err(error) => {
                    return Err(SnapshotError {
                        typedb_source: storage::snapshot::SnapshotError::Commit { typedb_source: error },
                    })
                }
            };

            // `None` means empty commit
            if let Some(sequence_number) = sequence_number {
                let type_cache = match TypeCache::new(self.storage.clone(), sequence_number) {
                    Ok(type_cache) => type_cache,
                    Err(typedb_source) => return Err(TypeCacheUpdateError { typedb_source }),
                };
                // replace Schema cache
                schema.type_cache = Arc::new(type_cache);
                let type_manager = TypeManager::new(
                    self.definition_key_generator.clone(),
                    self.type_vertex_generator.clone(),
                    Some(schema.type_cache.clone()),
                );
                let function_cache = match FunctionCache::new(self.storage.clone(), &type_manager, sequence_number) {
                    Ok(function_cache) => function_cache,
                    Err(typedb_source) => return Err(SchemaCommitError::FunctionError { typedb_source }),
                };
                schema.function_cache = Arc::new(function_cache);
                commit_profile.schema_update_caches_updated();
            }

            // replace statistics
            let mut thing_statistics = (*schema.thing_statistics).clone();

            if let Err(typedb_source) = thing_statistics.may_synchronise(&self.storage) {
                return Err(crate::transaction::SchemaCommitError::StatisticsError { typedb_source });
            }
            commit_profile.schema_update_statistics_keys_updated();

            schema.thing_statistics = Arc::new(thing_statistics);
            self.query_cache.force_reset(&schema.thing_statistics);

            *schema_commit_guard = schema;

            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn commit_record_exists(
        &self,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, DatabaseOpenError> {
        self.storage
            .commit_record_exists(open_sequence_number, snapshot_id)
            .map_err(|typedb_source| DatabaseOpenError::DurabilityClientRead { typedb_source })
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

    fn create(path: &Path, name: impl AsRef<str>) -> Result<Database<WALClient>, DatabaseOpenError> {
        use DatabaseOpenError::{
            DirectoryCreate, Encoding, FunctionCacheInitialise, StorageOpen, TypeCacheInitialise, WALOpen,
        };

        let name = name.as_ref();

        fs::create_dir(path).map_err(|source| DirectoryCreate { name: name.to_string(), source: Arc::new(source) })?;

        let wal = WAL::create(path).map_err(|source| WALOpen { source })?;
        let mut wal_client = WALClient::new(wal);
        wal_client.register_record_type::<Statistics>();

        let storage = Arc::new(
            MVCCStorage::create::<EncodingKeyspace>(name, path, wal_client)
                .map_err(|error| StorageOpen { typedb_source: error })?,
        );
        let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator =
            Arc::new(ThingVertexGenerator::load(storage.clone()).map_err(|err| Encoding { source: err })?);
        let thing_statistics = Arc::new(Statistics::new(storage.snapshot_watermark()));

        let type_cache = Arc::new(
            TypeCache::new(storage.clone(), SequenceNumber::MIN)
                .map_err(|error| TypeCacheInitialise { typedb_source: error })?,
        );

        let function_cache = Arc::new(
            FunctionCache::new(
                storage.clone(),
                &TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None),
                SequenceNumber::MIN,
            )
            .map_err(|error| FunctionCacheInitialise { typedb_source: error })?,
        );

        let schema = Arc::new(RwLock::new(Schema { thing_statistics, type_cache, function_cache }));
        let schema_txn_lock = Arc::new(RwLock::default());

        let query_cache = Arc::new(QueryCache::new());
        let update_statistics =
            make_update_statistics_fn(storage.clone(), schema.clone(), schema_txn_lock.clone(), query_cache.clone());
        let checkpoint_fn = make_checkpoint_fn(path.to_owned(), SequenceNumber::MIN, storage.clone());

        Ok(Database::<WALClient> {
            name: name.to_owned(),
            path: path.to_owned(),
            storage,
            definition_key_generator,
            type_vertex_generator,
            thing_vertex_generator,
            schema,
            query_cache,
            schema_write_transaction_exclusivity: Mutex::new((false, 0, VecDeque::with_capacity(100))),
            _statistics_updater: IntervalRunner::new(update_statistics, STATISTICS_UPDATE_INTERVAL),
            _checkpointer: IntervalRunner::new(checkpoint_fn, CHECKPOINT_INTERVAL),
        })
    }

    fn load(path: &Path, name: impl AsRef<str>) -> Result<Database<WALClient>, DatabaseOpenError> {
        use DatabaseOpenError::{
            CheckpointCreate, CheckpointLoad, DurabilityClientRead, Encoding, StatisticsInitialise, StorageOpen,
            TypeCacheInitialise, WALOpen,
        };
        let name = name.as_ref();
        event!(
            Level::TRACE,
            "Loading database '{}', at path '{:?}' (absolute path: '{:?}').",
            &name,
            path,
            std::path::absolute(path)
        );

        event!(Level::TRACE, "Loading database '{}' WAL.", &name);
        let wal = WAL::load(path).map_err(|source| WALOpen { source })?;
        let wal_last_sequence_number = wal.previous();

        let mut wal_client = WALClient::new(wal);
        wal_client.register_record_type::<Statistics>();

        event!(Level::TRACE, "Loading last database '{}' checkpoint", &name);
        let checkpoint = Checkpoint::open_latest(path)
            .map_err(|err| CheckpointLoad { name: name.to_string(), typedb_source: err })?;
        let storage = Arc::new(
            MVCCStorage::load::<EncodingKeyspace>(&name, path, wal_client, &checkpoint)
                .map_err(|error| StorageOpen { typedb_source: error })?,
        );
        let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator =
            Arc::new(ThingVertexGenerator::load(storage.clone()).map_err(|err| Encoding { source: err })?);

        event!(Level::TRACE, "Finding last database '{}' statistics WAL entry", &name);
        let mut thing_statistics = storage
            .durability()
            .find_last_unsequenced_type::<Statistics>()
            .map_err(|typedb_source| DurabilityClientRead { typedb_source })?
            .unwrap_or_else(|| Statistics::new(SequenceNumber::MIN));
        event!(
            Level::TRACE,
            "Synchronising database '{}' statistics from seq nr '{}'",
            &name,
            thing_statistics.sequence_number
        );
        thing_statistics.may_synchronise(&storage).map_err(|err| StatisticsInitialise { typedb_source: err })?;
        event!(Level::TRACE, "Thing statistics: {:?}", thing_statistics);
        let thing_statistics = Arc::new(thing_statistics);

        let type_cache = Arc::new(
            TypeCache::new(storage.clone(), wal_last_sequence_number)
                .map_err(|error| TypeCacheInitialise { typedb_source: error })?,
        );

        let function_cache = Arc::new(
            FunctionCache::new(
                storage.clone(),
                &TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None),
                wal_last_sequence_number,
            )
            .map_err(|error| FunctionCacheInitialise { typedb_source: error })?,
        );

        let schema = Arc::new(RwLock::new(Schema { thing_statistics, type_cache, function_cache }));
        let schema_txn_lock = Arc::new(RwLock::default());

        let checkpoint_sequence_number = match checkpoint {
            None => SequenceNumber::MIN,
            Some(checkpoint) => checkpoint
                .read_sequence_number()
                .map_err(|err| CheckpointLoad { name: name.to_string(), typedb_source: err })?,
        };

        let query_cache = Arc::new(QueryCache::new());
        let update_statistics =
            make_update_statistics_fn(storage.clone(), schema.clone(), schema_txn_lock.clone(), query_cache.clone());
        let checkpoint_fn = make_checkpoint_fn(path.to_owned(), checkpoint_sequence_number, storage.clone());

        let database = Database::<WALClient> {
            name: name.to_owned(),
            path: path.to_owned(),
            storage,
            definition_key_generator,
            type_vertex_generator,
            thing_vertex_generator,
            schema,
            query_cache,
            schema_write_transaction_exclusivity: Mutex::new((false, 0, VecDeque::with_capacity(100))),
            _statistics_updater: IntervalRunner::new(update_statistics, STATISTICS_UPDATE_INTERVAL),
            _checkpointer: IntervalRunner::new_with_initial_delay(
                checkpoint_fn,
                CHECKPOINT_INTERVAL,
                CHECKPOINT_INTERVAL,
            ),
        };

        if checkpoint_sequence_number < wal_last_sequence_number {
            database.checkpoint().map_err(|err| CheckpointCreate { name: name.to_string(), source: err })?;
        }
        event!(Level::TRACE, "Finished loading database '{}'", &name);
        Ok(database)
    }

    fn checkpoint(&self) -> Result<(), CheckpointCreateError> {
        let checkpoint = Checkpoint::new(&self.path)?;
        self.storage.checkpoint(&checkpoint)?;
        checkpoint.finish()?;
        Ok(())
    }

    #[allow(clippy::drop_non_drop)]
    pub fn delete(self) -> Result<(), DatabaseDeleteError> {
        drop(self._statistics_updater);
        drop(self._checkpointer);
        drop(Arc::into_inner(self.schema).expect("Cannot get exclusive ownership of inner of Arc<Schema>."));
        drop(Arc::into_inner(self.query_cache).expect("Cannot get exclusive ownership of inner of Arc<QueryCache>."));
        drop(
            Arc::into_inner(self.type_vertex_generator)
                .expect("Cannot get exclusive ownership of inner of Arc<TypeVertexGenerator>"),
        );
        drop(
            Arc::into_inner(self.thing_vertex_generator)
                .expect("Cannot get exclusive ownership of inner of Arc<ThingVertexGenerator>"),
        );
        drop(
            Arc::into_inner(self.definition_key_generator)
                .expect("Cannot get exclusive ownership of inner of Arc<DefinitionKeyGenerator>"),
        );
        Arc::into_inner(self.storage)
            .expect("Cannot get exclusive ownership of inner of Arc<MVCCStorage>.")
            .delete_storage()
            .map_err(|err| DatabaseDeleteError::StorageDelete { typedb_source: err })?;
        let path = self.path;
        fs::remove_dir_all(path).map_err(|err| DatabaseDeleteError::DirectoryDelete { source: Arc::new(err) })?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), DatabaseResetError> {
        use DatabaseResetError::CorruptionPartialResetStorageInUse;

        self.reserve_schema_transaction(Duration::from_secs(60).as_millis() as u64)
            .map_err(|typedb_source| DatabaseResetError::Transaction { typedb_source })?; // exclusively lock out other write or schema transactions;
        let mut locked_schema = self.schema.write().unwrap();

        match Arc::get_mut(&mut self.storage) {
            None => return Err(DatabaseResetError::StorageInUse {}),
            Some(storage) => {
                storage.reset().map_err(|err| CorruptionPartialResetStorageInUse { typedb_source: err })?
            }
        }
        match Arc::get_mut(&mut self.definition_key_generator) {
            None => return Err(CorruptionPartialResetKeyGeneratorInUse {}),
            Some(definition_key_generator) => definition_key_generator.reset(),
        }
        match Arc::get_mut(&mut self.type_vertex_generator) {
            None => return Err(CorruptionPartialResetTypeVertexGeneratorInUse {}),
            Some(type_vertex_generator) => type_vertex_generator.reset(),
        }
        match Arc::get_mut(&mut self.thing_vertex_generator) {
            None => return Err(CorruptionPartialResetThingVertexGeneratorInUse {}),
            Some(thing_vertex_generator) => thing_vertex_generator.reset(),
        }

        let thing_statistics = Arc::get_mut(&mut locked_schema.thing_statistics).unwrap();
        thing_statistics.reset(self.storage.snapshot_watermark());

        self.query_cache.force_reset(&Statistics::new(SequenceNumber::MIN));

        self.release_schema_transaction();
        Ok(())
    }

    pub fn get_metrics(&self) -> DatabaseMetrics {
        let schema = self.schema.read().expect("Expected database schema lock acquisition");
        DatabaseMetrics {
            database_name: self.name().to_owned(),
            schema: SchemaLoadMetrics { type_count: schema.type_cache.get_types_count() },
            data: DataLoadMetrics {
                entity_count: schema.thing_statistics.total_entity_count,
                relation_count: schema.thing_statistics.total_relation_count,
                attribute_count: schema.thing_statistics.total_attribute_count,
                has_count: schema.thing_statistics.total_has_count,
                role_count: schema.thing_statistics.total_role_count,
                storage_in_bytes: self.storage.estimate_size_in_bytes().expect("Expected storage size in bytes"),
                storage_key_count: self.storage.estimate_key_count().expect("Expected storage key count"),
            },
            is_primary_server: true, // TODO: Should be retrieved differently for Cloud
        }
    }
}

fn make_checkpoint_fn(
    path: PathBuf,
    mut prev_checkpoint: SequenceNumber,
    storage: Arc<MVCCStorage<WALClient>>,
) -> impl FnMut() {
    move || {
        let watermark = storage.snapshot_watermark();
        if prev_checkpoint < watermark {
            let checkpoint = Checkpoint::new(&path).unwrap();
            storage.checkpoint(&checkpoint).unwrap();
            checkpoint.finish().unwrap();
            prev_checkpoint = watermark;
        }
    }
}

fn make_update_statistics_fn(
    storage: Arc<MVCCStorage<WALClient>>,
    schema: Arc<RwLock<Schema>>,
    schema_txn_lock: Arc<RwLock<()>>,
    query_cache: Arc<QueryCache>,
) -> impl Fn() {
    move || {
        if storage.snapshot_watermark() > (*schema).read().unwrap().thing_statistics.sequence_number {
            let _schema_txn_guard = schema_txn_lock.read().unwrap(); // prevent Schema txns from opening during statistics update
            let mut new_statistics = (*schema.read().unwrap().thing_statistics).clone();
            new_statistics.may_synchronise(&storage).expect("Statistics sync failed");
            query_cache.may_evict(&new_statistics);
            schema.write().unwrap().thing_statistics = Arc::new(new_statistics);
        }
    }
}

typedb_error! {
    pub DatabaseOpenError(component = "Database open", prefix = "DBO") {
        InvalidUnicodeName(1, "Could not open database: invalid unicode name '{name:?}'.", name: OsString),
        DirectoryRead(2, "Error while reading directory for '{name}'.", name: String, source: Arc<io::Error>),
        DirectoryCreate(3, "Error creating directory for '{name}'", name: String, source: Arc<io::Error>),
        StorageOpen(4, "Error opening storage layer.", typedb_source: StorageOpenError),
        WALOpen(5, "Error opening WAL.", source: DurabilityServiceError),
        DurabilityClientOpen(6, "Error opening durability client.", typedb_source: DurabilityClientError),
        DurabilityClientRead(7, "Error reading from durability client.", typedb_source: DurabilityClientError),
        CheckpointLoad(8, "Error loading checkpoint for database '{name}'.", name: String, typedb_source: CheckpointLoadError),
        CheckpointCreate(9, "Error creating checkpoint for database '{name}'.", name: String, source: CheckpointCreateError),
        Encoding(10, "Data encoding error.", source: EncodingError),
        StatisticsInitialise(11, "Error initialising statistics manager.", typedb_source: StatisticsError),
        TypeCacheInitialise(12, "Error initialising type cache.", typedb_source: TypeCacheCreateError),
        FunctionCacheInitialise(13, "Error initialising function cache.", typedb_source: FunctionError),
        FileDelete(14, "Error while deleting file for '{name}'", name: String, source: Arc<io::Error>),
        DirectoryDelete(15, "Error while deleting directory of '{name}'", name: String, source: Arc<io::Error>),
    }
}

typedb_error! {
    pub DatabaseCreateError(component = "Database create", prefix = "DBC") {
        InvalidName(1, "Cannot create database since '{name}' is not a valid database name.", name: String),
        InternalDatabaseCreationProhibited(2, "Creating an internal database is prohibited."),
        DatabaseOpen(3, "Database open error.", typedb_source: DatabaseOpenError),
        WriteAccessDenied(4, "Cannot access databases for writing."),
        ReadAccessDenied(5, "Cannot access databases for reading."),
        AlreadyExists(6, "Database '{name}' already exists.", name: String),
        AlreadyExistsAndCleanupBlocked(7, "Database '{name}' already exists. Error while removing the imported duplicate.", name: String, typedb_source: DatabaseDeleteError),
        IsBeingImported(8, "Cannot create database '{name}' since it is being imported.", name: String),
        IsNotBeingImported(9, "Internal error: database '{name}' is not being imported.", name: String),
        DirectoryWrite(10, "Error while writing to data directory for '{name}'.", name: String, source: Arc<io::Error>),
        DatabaseMove(11, "Error while moving database {name} while finalization.", name: String),
    }
}

typedb_error! {
    pub DatabaseDeleteError(component = "Database delete", prefix = "DBD") {
        DoesNotExist(1, "Cannot delete database since it does not exist."),
        InUse(2, "Cannot delete database since it is in use."),
        StorageDelete(3, "Error while deleting storage resources.", typedb_source: StorageDeleteError),
        DirectoryDelete(4, "Error deleting directory.", source: Arc<io::Error>),
        InternalDatabaseDeletionProhibited(5, "Deleting an internal database is prohibited"),
        WriteAccessDenied(6, "Cannot access databases for writing."),
        DatabaseIsNotBeingImported(7, "Internal error: database '{name}' is not being imported.", name: String),
    }
}

typedb_error! {
    pub DatabaseResetError(component = "Database reset", prefix = "DBR") {
        DatabaseDelete(1, "Cannot delete database.", typedb_source: DatabaseDeleteError),
        DatabaseCreate(2, "Cannot create database.", typedb_source: DatabaseCreateError),
        Transaction(3, "Transaction error.", typedb_source: TransactionError),
        InUse(4, "Database cannot be reset since it is in use."),
        StorageInUse(5, "Database cannot be reset since the storage is in use."),
        CorruptionPartialResetStorageInUse(
            6,
            "Corruption warning: database reset failed partway because the storage is still in use.",
            typedb_source: StorageResetError
        ),
        CorruptionPartialResetKeyGeneratorInUse(
            7,
            "Corruption warning: Database reset failed partway because the schema key generator is still in use."
        ),
        CorruptionPartialResetTypeVertexGeneratorInUse(
            8,
            "Corruption warning: Database reset failed partway because the type key generator is still in use."
        ),
        CorruptionPartialResetThingVertexGeneratorInUse(
            9,
            "Corruption warning: Database reset failed partway because the instance key generator is still in use."
        ),
        CorruptionPartialResetQuertyCacheInUse(
            10,
            "Corruption warning: Database reset failed partway because the query cache is still in use."
        ),
    }
}
