/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, RwLock};

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use concept::error::ConceptWriteError;
use durability::DurabilityService;
use storage::snapshot::{CommittableSnapshot, ReadSnapshot, SchemaSnapshot, WritableSnapshot, WriteSnapshot};

use super::Database;

pub struct TransactionRead<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: ReadSnapshot<D>,
    pub(crate) type_manager: Arc<TypeManager<ReadSnapshot<D>>>,
    pub(crate) thing_manager: ThingManager<ReadSnapshot<D>>,
}

impl<D: DurabilityService> TransactionRead<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let snapshot: ReadSnapshot<D> = database.storage.clone().open_snapshot_read();
        let type_manager = Arc::new(TypeManager::new(database.type_vertex_generator.clone(), None)); // TODO pass cache
        let thing_manager =
            ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone());
        Self { database, snapshot, type_manager, thing_manager }
    }

    pub fn type_manager(&self) -> &TypeManager<ReadSnapshot<D>> {
        &self.type_manager
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources()
    }
}

pub struct TransactionWrite<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: WriteSnapshot<D>,
    pub(crate) type_manager: Arc<TypeManager<WriteSnapshot<D>>>,
    pub(crate) thing_manager: ThingManager<WriteSnapshot<D>>,
}

impl<D: DurabilityService> TransactionWrite<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let snapshot: WriteSnapshot<D> = database.storage.clone().open_snapshot_write();
        let type_manager = Arc::new(TypeManager::new(database.type_vertex_generator.clone(), None)); // TODO pass cache
        let thing_manager =
            ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone());
        Self { database, snapshot, type_manager, thing_manager }
    }

    pub fn type_manager(&self) -> &TypeManager<WriteSnapshot<D>> {
        &self.type_manager
    }

    pub fn thing_manager(&self) -> &ThingManager<WriteSnapshot<D>> {
        &self.thing_manager
    }

    pub fn commit(self) -> Result<(), Vec<ConceptWriteError>> {
        self.thing_manager.finalise()?;
        drop(self.type_manager);
        // TODO: pass error up
        self.snapshot.commit().unwrap_or_else(|_| { panic!("Failed to commit snapshot"); });
        Ok(())
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources();
    }
}

pub struct TransactionSchema<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: SchemaSnapshot<D>,
    pub(crate) type_manager: Arc<TypeManager<SchemaSnapshot<D>>>, // TODO: krishnan: Should this be an arc or direct ownership?
    pub(crate) thing_manager: ThingManager<SchemaSnapshot<D>>,
}

impl<D: DurabilityService> TransactionSchema<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let snapshot: SchemaSnapshot<D> = database.storage.clone().open_snapshot_schema();
        let type_manager = Arc::new(TypeManager::new(database.type_vertex_generator.clone(), None));
        let thing_manager =
            ThingManager::new(database.thing_vertex_generator.clone(), type_manager.clone());
        Self { database, snapshot, type_manager, thing_manager }
    }

    pub fn type_manager(&self) -> &TypeManager<SchemaSnapshot<D>> {
        &self.type_manager
    }

    pub fn commit(self) -> Result<(), Vec<ConceptWriteError>> {
        self.thing_manager.finalise()?;
        let type_manager_owned = Arc::try_unwrap(self.type_manager).unwrap_or_else(|_| { panic!("Failed to unwrap type_manager arc"); });
        type_manager_owned.finalise()?;
        self.snapshot.commit().unwrap_or_else(|_| { panic!("Failed to commit snapshot"); });
        Ok(())
    }

    pub fn close(self) {
        drop(self.thing_manager);
        drop(self.type_manager);
        self.snapshot.close_resources();
    }
}
