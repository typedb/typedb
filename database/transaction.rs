/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    fmt::Formatter,
    ops::Deref,
    sync::{mpsc::RecvTimeoutError, Arc},
};

use concept::{
    error::ConceptWriteError,
    thing::{statistics::StatisticsError, thing_manager::ThingManager},
    type_::type_manager::{type_cache::TypeCacheCreateError, TypeManager},
};
use error::typedb_error;
use function::{function_manager::FunctionManager, FunctionError};
use options::TransactionOptions;
use query::query_manager::QueryManager;
use resource::profile::TransactionProfile;
use storage::{
    durability_client::DurabilityClient,
    isolation_manager::CommitRecord,
    snapshot::{
        CommittableSnapshot, ReadSnapshot, SchemaSnapshot, SnapshotDropGuard, SnapshotError, WritableSnapshot,
        WriteSnapshot,
    },
    MVCCStorage,
};
use tracing::Level;

use crate::Database;

#[derive(Debug)]
pub struct TransactionRead<D> {
    pub snapshot: SnapshotDropGuard<ReadSnapshot<D>>,
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
            snapshot: SnapshotDropGuard::new(snapshot),
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
        &self.snapshot
    }

    pub fn close(self) {
        drop(self)
    }
}

#[derive(Debug)]
pub struct TransactionWrite<D> {
    pub snapshot: SnapshotDropGuard<WriteSnapshot<D>>,
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
            snapshot: SnapshotDropGuard::new(snapshot),
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
            snapshot: SnapshotDropGuard::from_arc(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        }
    }

    pub fn finalise(self) -> (TransactionProfile, Result<(DatabaseDropGuard<D>, WriteSnapshot<D>), DataCommitError>) {
        let mut profile = self.profile;
        let commit_profile = profile.commit_profile();

        let mut snapshot = match self.snapshot.try_into_inner() {
            None => return (profile, Err(DataCommitError::SnapshotInUse {})),
            Some(snapshot) => snapshot,
        };

        if let Err(errs) = self.thing_manager.finalise(&mut snapshot, commit_profile.storage_counters()) {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            let error = errs.into_iter().next().unwrap();
            return (profile, Err(DataCommitError::ConceptWriteErrorsFirst { typedb_source: Box::new(error) }));
        };
        commit_profile.things_finalised();
        drop(self.type_manager);
        (profile, Ok((self.database, snapshot)))
    }

    // TODO: remove this method and update the test accordingly
    pub fn commit(mut self) -> (TransactionProfile, Result<(), DataCommitError>) {
        self.profile.commit_profile().start();

        let (mut profile, (database, snapshot)) = match self.finalise() {
            (profile, Ok((database, snapshot))) => (profile, (database, snapshot)),
            (profile, Err(error)) => return (profile, Err(error)),
        };
        let result = database.data_commit_with_snapshot(snapshot, profile.commit_profile());
        profile.commit_profile().end();
        (profile, result)
    }

    pub fn rollback(&mut self) {
        self.snapshot.as_mut().expect("Expected owning snapshot on rollback").clear()
    }

    pub fn close(self) {
        drop(self)
    }
}

#[derive(Debug)]
pub struct TransactionSchema<D> {
    pub snapshot: SnapshotDropGuard<SchemaSnapshot<D>>,
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
            snapshot: SnapshotDropGuard::new(snapshot),
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
            snapshot: SnapshotDropGuard::from_arc(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        }
    }

    pub fn finalise(
        self,
    ) -> (TransactionProfile, Result<(DatabaseDropGuard<D>, SchemaSnapshot<D>), SchemaCommitError>) {
        use SchemaCommitError::{ConceptWriteErrorsFirst, FunctionError};

        let mut profile = self.profile;
        let commit_profile = profile.commit_profile();
        let mut snapshot = self.snapshot.into_inner();

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

        let type_manager = Arc::into_inner(self.type_manager).expect("Failed to unwrap Arc<TypeManager>");
        drop(type_manager);
        (profile, Ok((self.database, snapshot)))
    }

    // TODO: remove this method and update the test accordingly
    pub fn commit(mut self) -> (TransactionProfile, Result<(), SchemaCommitError>) {
        self.profile.commit_profile().start();

        let (mut profile, (database, snapshot)) = match self.finalise() {
            (profile, Ok((database, snapshot))) => (profile, (database, snapshot)),
            (profile, Err(error)) => return (profile, Err(error)),
        };
        let result = database.schema_commit_with_snapshot(snapshot, profile.commit_profile());
        profile.commit_profile().end();
        (profile, result)
    }

    pub fn rollback(&mut self) {
        self.snapshot.as_mut().expect("Expected owning snapshot on rollback").clear()
    }

    pub fn close(self) {
        drop(self)
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
        let mut $inner_snapshot = snapshot.into_inner();

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
