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

use crate::Database;

pub struct TransactionRead<D> {
    pub snapshot: ReadSnapshot<D>,
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

        Self { snapshot, type_manager, thing_manager, function_manager, _database: database }
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources()
    }
}

pub struct TransactionWrite<D> {
    pub snapshot: WriteSnapshot<D>,
    pub type_manager: Arc<TypeManager>,
    pub thing_manager: Arc<ThingManager>,
    pub function_manager: FunctionManager, // TODO: krishnan: Should this be an arc or direct ownership?

    // NOTE: The fields of a struct are dropped in declaration order. `_schema_txn_guard`
    // conceptually borrows `_database`, so it _must_ be dropped before `_database`,
    // and therefore _must_ be declared before `_database`.
    // See https://doc.rust-lang.org/reference/destructors.html
    _schema_txn_guard: RwLockReadGuard<'static, ()>,
    // prevents opening new schema txns while this txn is alive
    _database: Arc<Database<D>>,
}

impl<D: DurabilityClient> TransactionWrite<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let schema_txn_guard = unsafe {
            // SAFETY: `TransactionWrite` owns `Arc<Database>`, so the `schema_txn_lock`
            // is valid for the lifetime of the struct. The ordering of the fields
            // ensures that the lock is released before the database pointer is dropped.
            transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(database.schema_txn_lock.read().unwrap())
        };
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
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            _schema_txn_guard: schema_txn_guard,
            _database: database,
        }
    }

    pub fn commit(mut self) -> Result<(), Vec<ConceptWriteError>> {
        self.thing_manager.finalise(&mut self.snapshot)?;
        drop(self.type_manager);
        // TODO: pass error up
        self.snapshot.commit().unwrap_or_else(|_| panic!("Failed to commit snapshot"));
        Ok(())
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources();
    }
}

pub struct TransactionSchema<D> {
    pub snapshot: SchemaSnapshot<D>,
    pub type_manager: Arc<TypeManager>,
    // TODO: krishnan: Should this be an arc or direct ownership?
    pub thing_manager: ThingManager,
    pub function_manager: FunctionManager, // TODO: krishnan: Should this be an arc or direct ownership?

    // NOTE: The fields of a struct are dropped in declaration order. `_schema_guard` conceptually
    // borrows `_database`, so it _must_ be dropped before `_database`, and therefore _must_ be
    // declared before `_database`.
    // See https://doc.rust-lang.org/reference/destructors.html
    _schema_txn_guard: RwLockWriteGuard<'static, ()>,
    // prevents write txns while a schema txns running
    database: Arc<Database<D>>,
}

impl<D: DurabilityClient> TransactionSchema<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let schema_txn_guard = unsafe {
            // SAFETY: `TransactionSchema` owns `Arc<Database>`, so the `schema_lock` os valid for
            // the lifetime of the struct. The ordering of the fields ensures that the lock is
            // released before the database pointer is dropped.
            transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(
                database.schema_txn_lock.write().unwrap(),
            )
        };

        let snapshot: SchemaSnapshot<D> = database.storage.clone().open_snapshot_schema();
        let type_manager = Arc::new(TypeManager::new(
            database.definition_key_generator.clone(),
            database.type_vertex_generator.clone(),
            None,
        ));
        let thing_manager = ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone());
        let function_manager = FunctionManager::new(database.definition_key_generator.clone(), None);
        Self { snapshot, type_manager, thing_manager, function_manager, _schema_txn_guard: schema_txn_guard, database }
    }

    pub fn commit(mut self) -> Result<(), SchemaCommitError> {
        use SchemaCommitError::{ConceptWrite, Statistics, TypeCacheUpdate};

        self.thing_manager.finalise(&mut self.snapshot).map_err(|errors| ConceptWrite { errors })?;
        drop(self.thing_manager);

        self.function_manager
            .finalise(&self.snapshot, &self.type_manager)
            .map_err(|source| SchemaCommitError::FunctionError { source })?;

        let type_manager = Arc::into_inner(self.type_manager).expect("Failed to unwrap type_manager Arc");
        type_manager.finalise(&self.snapshot).map_err(|errors| ConceptWrite { errors })?;

        // Schema commits must wait for all other data operations to finish. No new read or write
        // transaction may open until the commit completes.
        let mut schema_commit_guard = self.database.schema.write().unwrap();
        let mut schema = (*schema_commit_guard).clone();

        let mut thing_statistics = (*schema.thing_statistics).clone();

        // 1. synchronise statistics
        thing_statistics.may_synchronise(&self.database.storage).map_err(|error| Statistics { source: error })?;
        // 2. flush statistics to WAL, guaranteeing a version of statistics is in WAL before schema can change
        thing_statistics.durably_write(&self.database.storage).map_err(|error| Statistics { source: error })?;

        let sequence_number = self.snapshot.commit().expect("Failed to commit snapshot");

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

        Ok(())
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources();
    }
}

#[derive(Debug)]
pub enum SchemaCommitError {
    ConceptWrite { errors: Vec<ConceptWriteError> },
    TypeCacheUpdate { source: TypeCacheCreateError },
    Statistics { source: StatisticsError },
    FunctionError { source: FunctionError },
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
