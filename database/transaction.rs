/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use core::fmt;
use std::{
    error::Error,
    mem::transmute,
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::AtomicU64;

use concept::{
    error::ConceptWriteError,
    thing::{statistics::StatisticsError, thing_manager::ThingManager},
    type_::type_manager::{
        type_cache::{TypeCache, TypeCacheCreateError},
        TypeManager,
    },
};
use function::{function_manager::FunctionManager, FunctionError};
use storage::{
    durability_client::DurabilityClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, SchemaSnapshot, WritableSnapshot, WriteSnapshot},
};
use storage::durability_client::WALClient;
use storage::snapshot::SnapshotError;

use crate::Database;

#[derive(Debug)]
pub struct TransactionRead<D> {
    pub snapshot: Arc<ReadSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: FunctionManager,
    // TODO: krishnan: Should this be an arc or direct ownership?
    _database: Arc<Database<D>>,
}

impl<D: DurabilityClient> TransactionRead<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
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
        )); // TODO pass cache
        let thing_manager = Arc::new(ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone()));
        let function_manager =
            FunctionManager::new(database.definition_key_generator.clone(), Some(schema.function_cache.clone()));

        drop(schema);

        Self { snapshot: Arc::new(snapshot), type_manager, thing_manager, function_manager, _database: database }
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
    pub function_manager: FunctionManager, // TODO: krishnan: Should this be an arc or direct ownership?
    pub database: Arc<Database<D>>,
}

impl<D: DurabilityClient> TransactionWrite<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        database.reserve_write_transaction();

        let schema = database.schema.read().unwrap();
        let snapshot: WriteSnapshot<D> = database.storage.clone().open_snapshot_write();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            Some(schema.type_cache.clone()),
        ));
        let thing_manager = Arc::new(ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone()));
        let function_manager =
            FunctionManager::new(database.definition_key_generator.clone(), Some(schema.function_cache.clone()));
        drop(schema);

        Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            database: database,
        }
    }

    pub fn from(
        snapshot: Arc<WriteSnapshot<D>>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: FunctionManager,
        _database: Arc<Database<D>>,
    ) -> Self {
        Self {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            database: _database,
        }
    }

    pub fn commit(mut self) -> Result<(), DataCommitError> {
        let mut snapshot = Arc::into_inner(self.snapshot).unwrap();
        self.thing_manager.finalise(&mut snapshot)
            .map_err(|errs| DataCommitError::ConceptWriteErrors { source: errs })?;
        drop(self.type_manager);
        snapshot.commit().map_err(|err| DataCommitError::SnapshotError { source: err })?;
        self.database.release_write_transaction();
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

// TODO this should be a TypeDB error, although it can contain many errors!?
#[derive(Debug)]
pub enum DataCommitError {
    ConceptWriteErrors { source: Vec<ConceptWriteError> },
    SnapshotError { source: SnapshotError },
}

impl Display for DataCommitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for DataCommitError {}

// TODO: when we use typedb_error!, how do we pring stack trace? If we use the stack trace of each of these, we'll end up with a tree!
//       If there's 1, we can use the stack trace, otherwise, we should list out all the errors?

#[derive(Debug)]
pub struct TransactionSchema<D> {
    pub snapshot: Arc<SchemaSnapshot<D>>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: FunctionManager,
    pub database: Arc<Database<D>>,
}

impl<D: DurabilityClient> TransactionSchema<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        database.reserve_schema_transaction();

        let snapshot: SchemaSnapshot<D> = database.storage.clone().open_snapshot_schema();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            None,
        ));
        let thing_manager = ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone());
        let function_manager = FunctionManager::new(database.definition_key_generator.clone(), None);
        Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager: Arc::new(thing_manager),
            function_manager,
            database,
        }
    }

    pub fn from(
        snapshot: SchemaSnapshot<D>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        function_manager: FunctionManager,
        database: Arc<Database<D>>,
    ) -> Self {
        Self {
            snapshot: Arc::new(snapshot),
            type_manager,
            thing_manager,
            function_manager,
            database,
        }
    }

    pub fn commit(mut self) -> Result<(), SchemaCommitError> {
        use SchemaCommitError::{ConceptWrite, Statistics, TypeCacheUpdate};
        let mut snapshot = Arc::into_inner(self.snapshot).unwrap();

        self.type_manager.validate(&self.snapshot).map_err(|errors| ConceptWrite { errors })?;

        self.thing_manager.finalise(&mut snapshot).map_err(|errors| ConceptWrite { errors })?;
        drop(self.thing_manager);

        self.function_manager
            .finalise(&snapshot, &self.type_manager)
            .map_err(|source| SchemaCommitError::FunctionError { source })?;

        let type_manager = Arc::into_inner(self.type_manager).expect("Failed to unwrap type_manager Arc");
        drop(type_manager);

        // Schema commits must wait for all other data operations to finish. No new read or write
        // transaction may open until the commit completes.
        let mut schema_commit_guard = self.database.schema.write().unwrap();
        let mut schema = (*schema_commit_guard).clone();

        let mut thing_statistics = (*schema.thing_statistics).clone();

        // 1. synchronise statistics
        thing_statistics.may_synchronise(&self.database.storage).map_err(|error| Statistics { source: error })?;
        // 2. flush statistics to WAL, guaranteeing a version of statistics is in WAL before schema can change
        thing_statistics.durably_write(&self.database.storage).map_err(|error| Statistics { source: error })?;

        let sequence_number = snapshot.commit()
            .map_err(|err| SchemaCommitError::SnapshotError { source: err })?;

        // `None` means empty commit
        if let Some(sequence_number) = sequence_number {
            // replace Schema cache
            schema.type_cache = Arc::new(
                TypeCache::new(self.database.storage.clone(), sequence_number)
                    .map_err(|error| TypeCacheUpdate { source: error })?,
            );
        }

        // replace statistics
        thing_statistics.may_synchronise(&self.database.storage).map_err(|error| Statistics { source: error })?;

        schema.thing_statistics = Arc::new(thing_statistics);

        *schema_commit_guard = schema;

        self.database.release_schema_transaction();
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

#[derive(Debug)]
pub enum SchemaCommitError {
    ConceptWrite { errors: Vec<ConceptWriteError> },
    TypeCacheUpdate { source: TypeCacheCreateError },
    Statistics { source: StatisticsError },
    FunctionError { source: FunctionError },
    SnapshotError { source: SnapshotError },
}

impl fmt::Display for SchemaCommitError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SchemaCommitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
