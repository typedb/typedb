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
    sync::Arc,
};
use concept::error::ConceptWriteError;

use concept::type_::type_manager::TypeManager;
use durability::DurabilityService;
use encoding::{
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    EncodingKeyspace,
};
use storage::{snapshot::WriteSnapshot, MVCCStorage, StorageRecoverError};

pub struct Database<D> {
    name: String,
    path: PathBuf,
    pub(super) storage: Arc<MVCCStorage<D>>,
    pub(super) type_vertex_generator: Arc<TypeVertexGenerator>,
    pub(super) thing_vertex_generator: Arc<ThingVertexGenerator>,
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
        let storage = Arc::new(
            MVCCStorage::recover::<EncodingKeyspace>(name, path).map_err(|error| StorageRecover { source: error })?,
        );
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        TypeManager::<WriteSnapshot<D>>::initialise_types(storage.clone(), type_vertex_generator.clone())
            .map_err(|err| { DatabaseRecoverError::SchemaInitialisation { source: err } })?;

        storage.checkpoint().unwrap();

        Ok(Self {
            name: name.to_owned(),
            path: path.to_owned(),
            storage,
            type_vertex_generator,
            thing_vertex_generator,
        })
    }
}

#[derive(Debug)]
pub enum DatabaseRecoverError {
    DirectoryCreate { path: PathBuf, source: io::Error },
    StorageRecover { source: StorageRecoverError },
    SchemaInitialisation { source: ConceptWriteError }
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
            Self::SchemaInitialisation { source } => Some(source),
        }
    }
}
