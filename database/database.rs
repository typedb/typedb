/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use core::fmt;
use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    rc::Rc,
};

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use durability::DurabilityService;
use encoding::{
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    EncodingKeyspace,
};
use storage::{error::MVCCStorageError, snapshot::Snapshot, MVCCStorage, StorageRecoverError};
use storage::snapshot::{ReadSnapshot, WriteSnapshot};

use crate::transaction::{TransactionRead, TransactionWrite};

pub struct Database<D> {
    name: String,
    path: PathBuf,
    storage: MVCCStorage<D>,
    type_vertex_generator: TypeVertexGenerator,
    thing_vertex_generator: ThingVertexGenerator,
}

impl<D> fmt::Debug for Database<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").field("name", &self.name).field("path", &self.path).finish_non_exhaustive()
    }
}

impl<D> Database<D> {
    pub fn recover(path: &Path, database_name: impl AsRef<str>) -> Result<Self, DatabaseRecoverError>
    where
        D: DurabilityService,
    {
        use DatabaseRecoverError::*;

        let name = database_name.as_ref();
        if !path.exists() {
            fs::create_dir(path).map_err(|error| DirectoryCreate { path: path.to_owned(), source: error })?;
        }
        let mut storage =
            MVCCStorage::recover::<EncodingKeyspace>(name, path).map_err(|error| StorageRecover { source: error })?;
        let type_vertex_generator = TypeVertexGenerator::new();
        let thing_vertex_generator = ThingVertexGenerator::new();
        TypeManager::<'_, WriteSnapshot<'_, D>>::initialise_types(&mut storage, &type_vertex_generator);

        storage.checkpoint().unwrap();

        Ok(Self {
            name: name.to_owned(),
            path: path.to_owned(),
            storage,
            type_vertex_generator,
            thing_vertex_generator,
        })
    }

    pub fn transaction_read(&self) -> TransactionRead<'_, '_, D> {
        let snapshot: Rc<ReadSnapshot<'_, D>> = Rc::new(self.storage.open_snapshot_read());
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &self.type_vertex_generator, None)); // TODO pass cache
        let thing_manager = ThingManager::new(snapshot.clone(), &self.thing_vertex_generator, type_manager.clone());
        TransactionRead { snapshot, type_manager, thing_manager }
    }

    fn transaction_write(&self) -> TransactionWrite<'_, '_, D> {
        let snapshot: Rc<WriteSnapshot<'_, D>> = Rc::new(self.storage.open_snapshot_write());
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &self.type_vertex_generator, None)); // TODO pass cache for data write txn
        let thing_manager = ThingManager::new(snapshot.clone(), &self.thing_vertex_generator, type_manager.clone());
        TransactionWrite { snapshot, type_manager, thing_manager }
    }
}

#[derive(Debug)]
pub enum DatabaseRecoverError {
    DirectoryCreate { path: PathBuf, source: io::Error },
    StorageRecover { source: StorageRecoverError },
}

impl fmt::Display for DatabaseRecoverError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseRecoverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DirectoryCreate { source, .. } => Some(source),
            Self::StorageRecover { source } => Some(source),
        }
    }
}
