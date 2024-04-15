/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use storage::snapshot::{ReadSnapshot, SchemaSnapshot, WriteSnapshot};

use super::Database;

pub struct TransactionRead<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: Arc<ReadSnapshot<D>>,
    pub(crate) type_manager: Arc<TypeManager<ReadSnapshot<D>>>,
    pub(crate) thing_manager: ThingManager<ReadSnapshot<D>>,
}

impl<D> TransactionRead<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let snapshot: Arc<ReadSnapshot<D>> = Arc::new(database.storage.clone().open_snapshot_read());
        let type_manager = Arc::new(TypeManager::new(snapshot.clone(), database.type_vertex_generator.clone(), None)); // TODO pass cache
        let thing_manager =
            ThingManager::new(snapshot.clone(), database.thing_vertex_generator.clone(), type_manager.clone());
        Self { database, snapshot, type_manager, thing_manager }
    }

    pub fn type_manager(&self) -> &TypeManager<ReadSnapshot<D>> {
        &self.type_manager
    }
}

pub struct TransactionWrite<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: Arc<WriteSnapshot<D>>,
    pub(crate) type_manager: Arc<TypeManager<WriteSnapshot<D>>>,
    pub(crate) thing_manager: ThingManager<WriteSnapshot<D>>,
}

impl<D> TransactionWrite<D> {
    pub fn open(database: Arc<Database<D>>) -> Self {
        let snapshot: Arc<WriteSnapshot<D>> = Arc::new(database.storage.clone().open_snapshot_write());
        let type_manager = Arc::new(TypeManager::new(snapshot.clone(), database.type_vertex_generator.clone(), None)); // TODO pass cache
        let thing_manager =
            ThingManager::new(snapshot.clone(), database.thing_vertex_generator.clone(), type_manager.clone());
        Self { database, snapshot, type_manager, thing_manager }
    }

    pub fn type_manager(&self) -> &TypeManager<WriteSnapshot<D>> {
        &self.type_manager
    }

    pub fn thing_manager(&self) -> &ThingManager<WriteSnapshot<D>> {
        &self.thing_manager // TODO: Sort this out.
    }

    pub fn commit(self) {}
}

pub struct TransactionSchema<D> {
    database: Arc<Database<D>>,
    pub(crate) snapshot: Arc<SchemaSnapshot<D>>,
    pub(crate) type_manager: Arc<TypeManager<SchemaSnapshot<D>>>,
    pub(crate) thing_manager: ThingManager<SchemaSnapshot<D>>,
}

impl<D> TransactionSchema<D> {
    pub fn type_manager(&self) -> &TypeManager<SchemaSnapshot<D>> {
        &self.type_manager
    }

    pub fn commit(self) {}
}
