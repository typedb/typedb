/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt,
    sync::{mpsc::RecvTimeoutError, Arc},
};

use concept::{
    error::ConceptWriteError,
    thing::{statistics::StatisticsError, thing_manager::ThingManager},
    type_::type_manager::{
        type_cache::{TypeCache, TypeCacheCreateError},
        TypeManager,
    },
};
use error::typedb_error;
use function::{function_manager::FunctionManager, FunctionError};
use options::TransactionOptions;
use storage::{
    durability_client::DurabilityClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, SchemaSnapshot, SnapshotError, WritableSnapshot, WriteSnapshot},
};

use crate::Database;

#[derive(Debug)]
pub struct TransactionRead<D> {
    pub snapshot: Arc<ReadSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    _database: Arc<Database<D>>,
    transaction_options: TransactionOptions,
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

        drop(schema);

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            _database: database,
            transaction_options,
        })
    }

    pub fn snapshot(&self) -> &ReadSnapshot<D> {
        &self.snapshot
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        Arc::into_inner(self.snapshot).unwrap().close_resources()
    }
}

#[derive(Debug)]
pub struct TransactionWrite<D> {
    pub snapshot: Arc<WriteSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    pub database: Arc<Database<D>>,
    pub transaction_options: TransactionOptions,
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
        drop(schema);

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            database,
            transaction_options,
        })
    }

    pub fn from(
        snapshot: Arc<WriteSnapshot<D>>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        database: Arc<Database<D>>,
        transaction_options: TransactionOptions,
    ) -> Self {
        Self { snapshot, type_manager, thing_manager, function_manager, database, transaction_options }
    }

    pub fn commit(self) -> Result<(), DataCommitError> {
        let database = self.database.clone(); // TODO: can we get away without cloning the database before?
        let result = self.try_commit();
        database.release_write_transaction();
        result
    }

    pub fn try_commit(self) -> Result<(), DataCommitError> {
        let mut snapshot = Arc::into_inner(self.snapshot).ok_or_else(|| DataCommitError::SnapshotInUse {})?;
        self.thing_manager.finalise(&mut snapshot).map_err(|errs| {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            let error = errs.into_iter().next().unwrap();
            DataCommitError::ConceptWriteErrorsFirst { typedb_source: error }
        })?;
        drop(self.type_manager);
        snapshot.commit().map_err(|err| DataCommitError::SnapshotError { typedb_source: err })?;
        Ok(())
    }

    pub fn rollback(&mut self) {
        Arc::get_mut(&mut self.snapshot).unwrap().clear()
    }

    pub fn close(self) {
        self.database.release_write_transaction();
        drop(self.thing_manager);
        drop(self.type_manager);
        Arc::into_inner(self.snapshot).unwrap().close_resources()
    }
}

typedb_error!(
    pub DataCommitError(component = "Data commit", prefix = "DCT") {
        SnapshotInUse(1, "Failed to commit since the transaction snapshot is still in use."),
        ConceptWriteErrors(2, "Data commit error.", source: Vec<ConceptWriteError> ),
        ConceptWriteErrorsFirst(3, "Data commit error.", ( typedb_source : ConceptWriteError )),
        SnapshotError(4, "Snapshot error.", ( typedb_source: SnapshotError )),
    }
);

// #[derive(Debug, Clone)]
// pub enum DataCommitError {
// }
//
// impl fmt::Display for DataCommitError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         fmt::Debug::fmt(self, f)
//     }
// }
//
// impl Error for DataCommitError {}

// TODO: when we use typedb_error!, how do we pring stack trace? If we use the stack trace of each of these, we'll end up with a tree!
//       If there's 1, we can use the stack trace, otherwise, we should list out all the errors?

#[derive(Debug)]
pub struct TransactionSchema<D> {
    pub snapshot: Arc<SchemaSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: Arc<FunctionManager>,
    pub database: Arc<Database<D>>,
    pub transaction_options: TransactionOptions,
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

        Ok(Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager: Arc::new(thing_manager),
            function_manager,
            database,
            transaction_options,
        })
    }

    pub fn from(
        snapshot: SchemaSnapshot<D>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        database: Arc<Database<D>>,
        transaction_options: TransactionOptions,
    ) -> Self {
        Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            database,
            transaction_options,
        }
    }

    pub fn commit(self) -> Result<(), SchemaCommitError> {
        let database = self.database.clone(); // TODO: can we get away without cloning the database before?
        let result = self.try_commit();
        database.release_schema_transaction();
        result
    }

    fn try_commit(self) -> Result<(), SchemaCommitError> {
        use SchemaCommitError::{
            ConceptWriteErrorsFirst, FunctionError, SnapshotError, StatisticsError, TypeCacheUpdateError,
        };
        let mut snapshot = Arc::into_inner(self.snapshot).expect("Failed to unwrap Arc<Snapshot>");
        self.type_manager.validate(&snapshot).map_err(|errs| {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            ConceptWriteErrorsFirst { typedb_source: errs.into_iter().next().unwrap() }
        })?;

        self.thing_manager.finalise(&mut snapshot).map_err(|errs| {
            // TODO: send all the errors, not just the first,
            // when we can print the stacktraces of multiple errors, not just a single one
            ConceptWriteErrorsFirst { typedb_source: errs.into_iter().next().unwrap() }
        })?;
        drop(self.thing_manager);

        let function_manager = Arc::into_inner(self.function_manager).expect("Failed to unwrap Arc<FunctionManager>");
        function_manager
            .finalise(&snapshot, &self.type_manager)
            .map_err(|typedb_source| FunctionError { typedb_source })?;

        let type_manager = Arc::into_inner(self.type_manager).expect("Failed to unwrap Arc<TypeManager>");
        drop(type_manager);

        // Schema commits must wait for all other data operations to finish. No new read or write
        // transaction may open until the commit completes.
        let mut schema_commit_guard = self.database.schema.write().unwrap();
        let mut schema = (*schema_commit_guard).clone();

        let mut thing_statistics = (*schema.thing_statistics).clone();

        // 1. synchronise statistics
        thing_statistics
            .may_synchronise(&self.database.storage)
            .map_err(|typedb_source| StatisticsError { typedb_source })?;
        // 2. flush statistics to WAL, guaranteeing a version of statistics is in WAL before schema can change
        thing_statistics
            .durably_write(&self.database.storage)
            .map_err(|typedb_source| StatisticsError { typedb_source })?;

        let sequence_number = snapshot.commit().map_err(|typedb_source| SnapshotError { typedb_source })?;

        // `None` means empty commit
        if let Some(sequence_number) = sequence_number {
            // replace Schema cache
            schema.type_cache = Arc::new(
                TypeCache::new(self.database.storage.clone(), sequence_number)
                    .map_err(|typedb_source| TypeCacheUpdateError { typedb_source })?,
            );
        }

        // replace statistics
        thing_statistics
            .may_synchronise(&self.database.storage)
            .map_err(|typedb_source| StatisticsError { typedb_source })?;
        schema.thing_statistics = Arc::new(thing_statistics);

        *schema_commit_guard = schema;
        Ok(())
    }

    pub fn rollback(&mut self) {
        Arc::get_mut(&mut self.snapshot).unwrap().clear()
    }

    pub fn close(self) {
        self.database.release_schema_transaction();
        drop(self.thing_manager);
        drop(self.type_manager);
        Arc::into_inner(self.snapshot).unwrap().close_resources();
    }
}

// TODO: Same issue with ConceptWriteErrors vs ErrorsFirst as for DataCommitError
typedb_error!(
    pub SchemaCommitError(component = "Schema commit", prefix = "SCT") {
        ConceptWriteErrors(1, "Schema commit error.", source: Vec<ConceptWriteError> ),
        ConceptWriteErrorsFirst(2, "Schema commit error.", ( typedb_source : ConceptWriteError )),
        TypeCacheUpdateError(3, "TypeCache update error.", ( typedb_source: TypeCacheCreateError )),
        StatisticsError(4, "Statistics error.", ( typedb_source: StatisticsError )),
        FunctionError(5, "Function error.", ( typedb_source: FunctionError )),
        SnapshotError(6, "Snapshot error.", ( typedb_source: SnapshotError )),
    }
);

typedb_error!(
    pub TransactionError(component = "Transaction", prefix = "TXN") {
        Timeout(1, "Transaction timeout.", source: RecvTimeoutError),
        WriteExclusivityTimeout(2, "Transaction timeout due to an exclusive write access requested by this or a concurrent transaction."),
    }
);
