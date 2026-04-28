/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    fmt::Formatter,
    ops::Deref,
    sync::{Arc, mpsc::RecvTimeoutError},
};

use concept::{
    error::ConceptWriteError,
    thing::{statistics::StatisticsError, thing_manager::ThingManager},
    type_::type_manager::{
        TypeManager,
        type_cache::{TypeCache, TypeCacheCreateError},
    },
};
use durability::DurabilitySequenceNumber;
use error::typedb_error;
use function::{FunctionError, function_cache::FunctionCache, function_manager::FunctionManager};
use options::TransactionOptions;
use query::query_manager::QueryManager;
use resource::profile::{CommitProfile, TransactionProfile};
use storage::{
    durability_client::DurabilityClient,
    record::CommitRecord,
    snapshot::{
        snapshot_id::SnapshotId, CommittableSnapshot, ReadSnapshot, ReadableSnapshot, SchemaSnapshot, SnapshotError,
        WritableSnapshot, WriteSnapshot,
    },
};
use tracing::{Level};

use crate::Database;

pub trait CommitIntent: Sized {
    type Error;

    fn database_name(&self) -> &str;

    fn has_changes(&self) -> bool;

    fn commit(self, commit_profile: &mut CommitProfile) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct TransactionRead<D> {
    pub snapshot: Arc<ReadSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    pub query_manager: Arc<QueryManager>,
    pub database: DatabaseDropGuard<D>,
    transaction_options: TransactionOptions,
    pub profile: TransactionProfile,
}

impl<D: DurabilityClient> TransactionRead<D> {
    pub fn open(database: Arc<Database<D>>, transaction_options: TransactionOptions) -> Result<Self, TransactionError> {
        // TODO: when we implement constructor `open_at`, to open a transaction in the past by
        //      time/sequence number, we need to check whether
        //       the statistics that is available is "too far" ahead of the version we're opening (100-1000?)
        //          note: this can also be the approximate frequency at which we persist statistics snapshots to the WAL!
        //       this should be a constant defined in constants.rs
        //       If it's too far in the future, we should find a more appropriate statistics snapshot from the WAL
        let schema = database.schema.read().unwrap();
        let snapshot: ReadSnapshot<D> = database.storage.clone().open_snapshot_read();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            Some(schema.type_cache.clone()),
        ));
        let thing_manager = Arc::new(ThingManager::new(
            database.thing_vertex_generator.clone(),
            type_manager.clone(),
            schema.thing_statistics.clone(),
        ));
        let function_manager = Arc::new(FunctionManager::new(
            database.definition_key_generator.clone(),
            Some(schema.function_cache.clone()),
        ));
        let query_manager = Arc::new(QueryManager::new(Some(database.query_cache.clone())));

        drop(schema);

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database: DatabaseDropGuard::new(database),
            transaction_options,
            profile: TransactionProfile::new(tracing::enabled!(Level::TRACE)),
        })
    }

    pub fn snapshot(&self) -> &ReadSnapshot<D> {
        &*self.snapshot
    }

    pub fn close(self) {
        drop(self)
    }

    pub fn id(&self) -> TransactionId {
        TransactionId::new(self.snapshot.open_sequence_number(), self.snapshot.id())
    }
}

#[derive(Debug)]
pub struct TransactionWrite<D> {
    pub snapshot: Arc<WriteSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    pub query_manager: Arc<QueryManager>,
    pub database: DatabaseDropGuard<D>,
    pub transaction_options: TransactionOptions,
    pub profile: TransactionProfile,
}

impl<D: DurabilityClient> TransactionWrite<D> {
    pub fn open(database: Arc<Database<D>>, transaction_options: TransactionOptions) -> Result<Self, TransactionError> {
        database.reserve_write_transaction(transaction_options.schema_lock_acquire_timeout_millis)?;

        let schema = database.schema.read().unwrap();
        let snapshot: WriteSnapshot<D> = database.storage.clone().open_snapshot_write();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            Some(schema.type_cache.clone()),
        ));
        let thing_manager = Arc::new(ThingManager::new(
            database.thing_vertex_generator.clone(),
            type_manager.clone(),
            schema.thing_statistics.clone(),
        ));
        let function_manager = Arc::new(FunctionManager::new(
            database.definition_key_generator.clone(),
            Some(schema.function_cache.clone()),
        ));
        let query_manager = Arc::new(QueryManager::new(Some(database.query_cache.clone())));
        drop(schema);

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database: DatabaseDropGuard::new_with_fn(database, Database::release_write_transaction),
            transaction_options,
            profile: TransactionProfile::new(tracing::enabled!(Level::TRACE)),
        })
    }

    pub fn from_parts(
        snapshot: Arc<WriteSnapshot<D>>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: Arc<QueryManager>,
        database: DatabaseDropGuard<D>,
        transaction_options: TransactionOptions,
        profile: TransactionProfile,
    ) -> Self {
        Self {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        }
    }

    pub fn finalise(self) -> (TransactionProfile, Result<DataCommitIntent<D>, DataCommitError>) {
        let mut profile = self.profile;
        let commit_profile = profile.commit_profile();
        commit_profile.start();
        let mut snapshot = match Arc::try_unwrap(self.snapshot) {
            Err(_) => return (profile, Err(DataCommitError::SnapshotInUse {})),
            Ok(snapshot) => snapshot,
        };

        if let Err(errs) = self.thing_manager.finalise(&mut snapshot, commit_profile.storage_counters()) {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            let error = errs.into_iter().next().unwrap();
            return (profile, Err(DataCommitError::ConceptWriteErrorsFirst { typedb_source: Box::new(error) }));
        };
        commit_profile.things_finalised();
        (profile, Ok(DataCommitIntent { database_drop_guard: self.database, write_snapshot: snapshot }))
    }

    pub fn rollback(&mut self) {
        Arc::get_mut(&mut self.snapshot).expect("Expected owning snapshot on rollback").clear()
    }

    pub fn close(self) {
        drop(self)
    }

    pub fn id(&self) -> TransactionId {
        TransactionId::new(self.snapshot.open_sequence_number(), self.snapshot.id())
    }
}

#[derive(Debug)]
pub struct TransactionSchema<D> {
    pub snapshot: Arc<SchemaSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    pub query_manager: Arc<QueryManager>,
    pub database: DatabaseDropGuard<D>,
    pub transaction_options: TransactionOptions,
    pub profile: TransactionProfile,
}

impl<D: DurabilityClient> TransactionSchema<D> {
    pub fn open(database: Arc<Database<D>>, transaction_options: TransactionOptions) -> Result<Self, TransactionError> {
        database.reserve_schema_transaction(transaction_options.schema_lock_acquire_timeout_millis)?;

        let snapshot: SchemaSnapshot<D> = database.storage.clone().open_snapshot_schema();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            None,
        ));
        let thing_manager = {
            let schema = database.schema.read().unwrap();
            ThingManager::new(
                database.thing_vertex_generator.clone(),
                type_manager.clone(),
                schema.thing_statistics.clone(),
            )
        };
        let function_manager = Arc::new(FunctionManager::new(database.definition_key_generator.clone(), None));
        let query_manager = Arc::new(QueryManager::new(None));

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager: Arc::new(thing_manager),
            function_manager,
            query_manager,
            database: DatabaseDropGuard::new_with_fn(database, Database::release_schema_transaction),
            transaction_options,
            profile: TransactionProfile::new(tracing::enabled!(Level::TRACE)),
        })
    }

    pub fn from_parts(
        snapshot: Arc<SchemaSnapshot<D>>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: Arc<QueryManager>,
        database: DatabaseDropGuard<D>,
        transaction_options: TransactionOptions,
        profile: TransactionProfile,
    ) -> Self {
        Self {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        }
    }

    pub fn finalise(self) -> (TransactionProfile, Result<SchemaCommitIntent<D>, SchemaCommitError>) {
        use SchemaCommitError::{ConceptWriteErrorsFirst, FunctionError};

        let mut profile = self.profile;
        let commit_profile = profile.commit_profile();
        commit_profile.start();
        let mut snapshot = Arc::try_unwrap(self.snapshot)
            .unwrap_or_else(|_| panic!("Expected unique ownership of snapshot for schema commit"));
        if let Err(errs) = self.type_manager.validate(&snapshot) {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            return (
                profile,
                Err(ConceptWriteErrorsFirst { typedb_source: Box::new(errs.into_iter().next().unwrap()) }),
            );
        };
        commit_profile.types_validated();

        if let Err(errs) = self.thing_manager.finalise(&mut snapshot, commit_profile.storage_counters()) {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            return (
                profile,
                Err(ConceptWriteErrorsFirst { typedb_source: Box::new(errs.into_iter().next().unwrap()) }),
            );
        };
        commit_profile.things_finalised();
        drop(self.thing_manager);

        let function_manager = Arc::into_inner(self.function_manager).expect("Failed to unwrap Arc<FunctionManager>");
        if let Err(typedb_source) = function_manager.finalise(&snapshot, &self.type_manager) {
            return (profile, Err(FunctionError { typedb_source }));
        }
        commit_profile.functions_finalised();

        let _type_manager = Arc::into_inner(self.type_manager).expect("Failed to unwrap Arc<TypeManager>");
        (profile, Ok(SchemaCommitIntent { database_drop_guard: self.database, schema_snapshot: snapshot }))
    }

    pub fn rollback(&mut self) {
        Arc::get_mut(&mut self.snapshot).expect("Expected owning snapshot on rollback").clear()
    }

    pub fn close(self) {
        drop(self)
    }

    pub fn id(&self) -> TransactionId {
        TransactionId::new(self.snapshot.open_sequence_number(), self.snapshot.id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId {
    sequence_number: DurabilitySequenceNumber,
    snapshot_id: SnapshotId,
}

impl TransactionId {
    fn new(sequence_number: DurabilitySequenceNumber, snapshot_id: SnapshotId) -> Self {
        Self { sequence_number, snapshot_id }
    }

    pub fn sequence_number(&self) -> DurabilitySequenceNumber {
        self.sequence_number
    }

    pub fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }
}

#[macro_export]
macro_rules! with_transaction_parts {
    (
        $TransactionType:ident, $transaction:ident, |$inner_snapshot:ident, $type_manager:ident, $thing_manager:ident, $function_manager:ident, $query_manager:ident| $expr:expr
    ) => {{
        let $TransactionType {
            snapshot,
            type_manager: $type_manager,
            thing_manager: $thing_manager,
            function_manager: $function_manager,
            query_manager: $query_manager,
            database,
            transaction_options,
            profile,
        } = $transaction;
        let mut $inner_snapshot =
            Arc::try_unwrap(snapshot).unwrap_or_else(|_| panic!("Expected unique ownership of snapshot"));

        let result = $expr;

        let $transaction = $TransactionType::from_parts(
            Arc::new($inner_snapshot),
            $type_manager,
            $thing_manager,
            $function_manager,
            $query_manager,
            database,
            transaction_options,
            profile,
        );

        ($transaction, result)
    }};
}

pub struct DataCommitIntent<D> {
    database_drop_guard: DatabaseDropGuard<D>,
    write_snapshot: WriteSnapshot<D>,
}

impl<D: DurabilityClient> DataCommitIntent<D> {
    pub fn new(database: Arc<Database<D>>, write_snapshot: WriteSnapshot<D>) -> Self {
        Self { database_drop_guard: DatabaseDropGuard::new(database), write_snapshot }
    }

    pub fn from_commit_record(database: Arc<Database<D>>, commit_record: CommitRecord) -> Self {
        let snapshot = WriteSnapshot::new_with_commit_record(database.storage.clone(), commit_record);
        Self::new(database, snapshot)
    }

    pub fn into_commit_record(
        self,
    ) -> (DatabaseDropGuard<D>, storage::isolation_manager::ReaderDropGuard, CommitRecord) {
        let (reader_guard, commit_record) = self.write_snapshot.into_commit_record();
        (self.database_drop_guard, reader_guard, commit_record)
    }
}

impl<D: DurabilityClient> CommitIntent for DataCommitIntent<D> {
    type Error = DataCommitError;

    fn database_name(&self) -> &str {
        self.database_drop_guard.name()
    }

    fn has_changes(&self) -> bool {
        self.write_snapshot.has_changes()
    }

    fn commit(self, commit_profile: &mut CommitProfile) -> Result<(), DataCommitError> {
        self.write_snapshot
            .commit(commit_profile)
            .map(|_| ())
            .map_err(|typedb_source| DataCommitError::SnapshotError { typedb_source })
    }
}

pub struct SchemaCommitIntent<D> {
    database_drop_guard: DatabaseDropGuard<D>,
    schema_snapshot: SchemaSnapshot<D>,
}

impl<D: DurabilityClient> SchemaCommitIntent<D> {
    pub fn new(database: Arc<Database<D>>, schema_snapshot: SchemaSnapshot<D>) -> Self {
        Self { database_drop_guard: DatabaseDropGuard::new(database), schema_snapshot }
    }

    pub fn from_commit_record(database: Arc<Database<D>>, commit_record: CommitRecord) -> Self {
        let snapshot = SchemaSnapshot::new_with_commit_record(database.storage.clone(), commit_record);
        Self::new(database, snapshot)
    }

    pub fn into_commit_record(
        self,
    ) -> (DatabaseDropGuard<D>, storage::isolation_manager::ReaderDropGuard, CommitRecord) {
        let (reader_guard, commit_record) = self.schema_snapshot.into_commit_record();
        (self.database_drop_guard, reader_guard, commit_record)
    }
}

impl<D: DurabilityClient> CommitIntent for SchemaCommitIntent<D> {
    type Error = SchemaCommitError;

    fn database_name(&self) -> &str {
        self.database_drop_guard.name()
    }

    fn has_changes(&self) -> bool {
        self.schema_snapshot.has_changes()
    }

    fn commit(self, commit_profile: &mut CommitProfile) -> Result<(), SchemaCommitError> {
        use SchemaCommitError::{StatisticsError, TypeCacheUpdateError};
        let database = &self.database_drop_guard;

        // Schema commits must wait for all other data operations to finish. No new read or write
        // transaction may open until the commit completes.
        let mut schema_commit_guard = database.schema.write().unwrap();
        let mut schema = (*schema_commit_guard).clone();

        let mut thing_statistics = (*schema.thing_statistics).clone();

        // synchronise statistics
        if let Err(typedb_source) = thing_statistics.may_synchronise(&database.storage) {
            return Err(StatisticsError { typedb_source });
        }

        // flush statistics to WAL, guaranteeing a version of statistics is in WAL before schema can change
        if let Err(typedb_source) = thing_statistics.durably_write(database.storage.durability()) {
            return Err(StatisticsError { typedb_source });
        }
        commit_profile.schema_update_statistics_durably_written();

        let sequence_number = match self.schema_snapshot.commit(commit_profile) {
            Ok(sequence_number) => sequence_number,
            Err(typedb_source) => return Err(SchemaCommitError::SnapshotError { typedb_source }),
        };

        if let Some(sequence_number) = sequence_number {
            // replace schema cache
            let type_cache = match TypeCache::new(database.storage.clone(), sequence_number) {
                Ok(type_cache) => type_cache,
                Err(typedb_source) => return Err(TypeCacheUpdateError { typedb_source }),
            };
            schema.type_cache = Arc::new(type_cache);
            let type_manager = TypeManager::new(
                database.definition_key_generator.clone(),
                database.type_vertex_generator.clone(),
                Some(schema.type_cache.clone()),
            );
            let function_cache = match FunctionCache::new(database.storage.clone(), &type_manager, sequence_number) {
                Ok(function_cache) => function_cache,
                Err(typedb_source) => return Err(SchemaCommitError::FunctionError { typedb_source }),
            };
            schema.function_cache = Arc::new(function_cache);
            commit_profile.schema_update_caches_updated();
        }

        // replace statistics
        if let Err(typedb_source) = thing_statistics.may_synchronise(&database.storage) {
            return Err(StatisticsError { typedb_source });
        }
        commit_profile.schema_update_statistics_keys_updated();
        schema.thing_statistics = Arc::new(thing_statistics);

        database.query_cache.force_reset(&schema.thing_statistics);

        *schema_commit_guard = schema;
        Ok(())
    }
}

pub struct DatabaseDropGuard<D> {
    database: Option<Arc<Database<D>>>,
    on_drop_fn: Option<fn(&Database<D>)>,
}

impl<D> DatabaseDropGuard<D> {
    pub fn new(database: Arc<Database<D>>) -> Self {
        Self { database: Some(database), on_drop_fn: None }
    }

    pub fn new_with_fn(database: Arc<Database<D>>, on_drop_fn: fn(&Database<D>)) -> Self {
        Self { database: Some(database), on_drop_fn: Some(on_drop_fn) }
    }

    fn database(&self) -> &Arc<Database<D>> {
        self.database.as_ref().expect("Expected a database in the guard")
    }
}

impl<D> Deref for DatabaseDropGuard<D> {
    type Target = Arc<Database<D>>;

    fn deref(&self) -> &Self::Target {
        self.database()
    }
}

impl<D> Drop for DatabaseDropGuard<D> {
    fn drop(&mut self) {
        if let Some(on_drop_fn) = self.on_drop_fn.take() {
            let database = self.database.take().expect("Expected database");
            on_drop_fn(&database);
        }
    }
}

impl<D> std::fmt::Debug for DatabaseDropGuard<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.database)
    }
}

// TODO: when we use typedb_error!, how do we print stack trace? If we use the stack trace of each of these, we'll end up with a tree!
//       If there's 1, we can use the stack trace, otherwise, we should list out all the errors?

typedb_error! {
    pub DataCommitError(component = "Data commit", prefix = "DCT") {
        SnapshotInUse(1, "Failed to commit since the transaction snapshot is still in use."),
        ConceptWriteErrors(2, "Data commit error.", write_errors: Vec<ConceptWriteError>),
        ConceptWriteErrorsFirst(3, "Data commit error.", typedb_source: Box<ConceptWriteError>),
        SnapshotError(4, "Snapshot error.", typedb_source: SnapshotError),
    }
}

// TODO: Same issue with ConceptWriteErrors vs ErrorsFirst as for DataCommitError
typedb_error! {
    pub SchemaCommitError(component = "Schema commit", prefix = "SCT") {
        ConceptWriteErrors(1, "Schema commit error.", write_errors: Vec<ConceptWriteError>),
        ConceptWriteErrorsFirst(2, "Schema commit error.", typedb_source: Box<ConceptWriteError>),
        TypeCacheUpdateError(3, "TypeCache update error.", typedb_source: TypeCacheCreateError),
        StatisticsError(4, "Statistics error.", typedb_source: StatisticsError),
        FunctionError(5, "Function error.", typedb_source: FunctionError),
        SnapshotError(6, "Snapshot error.", typedb_source: SnapshotError),
    }
}

typedb_error! {
    pub TransactionError(component = "Transaction", prefix = "TXN") {
        Timeout(1, "Transaction timeout.", source: RecvTimeoutError),
        WriteExclusivityTimeout(2, "Transaction timeout due to an exclusive write access requested by this or a concurrent transaction."),
    }
}
