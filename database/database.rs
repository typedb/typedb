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
use std::ffi::OsString;

use concept::{error::ConceptWriteError, type_::type_manager::TypeManager};
use concept::thing::statistics::Statistics;
use durability::{DurabilityError, DurabilityService};
use durability::wal::WAL;
use encoding::{
    EncodingKeyspace,
    error::EncodingError,
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
};
use storage::{
    MVCCStorage,
    snapshot::WriteSnapshot, StorageOpenError,
};
use storage::recovery::checkpoint::{Checkpoint, CheckpointLoadError};

use crate::DatabaseOpenError::{DirectoryCreate, Encoding, SchemaInitialise, StorageOpen};

pub struct Database<D> {
    name: String,
    path: PathBuf,
    pub(super) storage: Arc<MVCCStorage<D>>,
    pub(super) type_vertex_generator: Arc<TypeVertexGenerator>,
    pub(super) thing_vertex_generator: Arc<ThingVertexGenerator>,
    // thing_statistics: Arc<Statistics>,
}

impl<D> fmt::Debug for Database<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").field("name", &self.name).field("path", &self.path).finish_non_exhaustive()
    }
}

impl<D> Database<D> {
    pub fn open(path: &Path) -> Result<Database<WAL>, DatabaseOpenError> {
        let file_name = path.file_name().unwrap();
        let name = file_name.to_str().ok_or_else(|| DatabaseOpenError::InvalidUnicodeName { name: file_name.to_owned() })?;

        if !path.exists() {
            Self::create(path, name)
        } else {
            Self::load(path, name)
        }
    }

    fn create(path: &Path, name: impl AsRef<str>) -> Result<Database<WAL>, DatabaseOpenError> {
        use DatabaseOpenError::{DirectoryCreate, Encoding, SchemaInitialise, StorageOpen, DurabilityOpen};
        fs::create_dir(path).map_err(|error| DirectoryCreate { path: path.to_owned(), source: error })?;
        let wal = WAL::create(&path).map_err(|err| DurabilityOpen { source: err })?;
        let storage = Arc::new(MVCCStorage::create::<EncodingKeyspace>(&name, path, wal)
            .map_err(|error| StorageOpen { source: error })?);
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone())
            .map_err(|err| Encoding { source: err })?);
        TypeManager::<WriteSnapshot<D>>::initialise_types(storage.clone(), type_vertex_generator.clone())
            .map_err(|err| SchemaInitialise { source: err })?;
        // let statistics = Statistics::new(storage.);

        Ok(Database::<WAL> {
            name: name.as_ref().to_owned(),
            path: path.to_owned(),
            storage,
            type_vertex_generator,
            thing_vertex_generator,
        })

    }

    fn load(path: &Path, name: impl AsRef<str>) -> Result<Database<WAL>, DatabaseOpenError> {
        use DatabaseOpenError::{Encoding, SchemaInitialise, StorageOpen, DurabilityOpen, CheckpointLoad};

        let wal = WAL::load(&path).map_err(|err| DurabilityOpen { source: err })?;
        let checkpoint = Checkpoint::open_latest(&path)
            .map_err(|err| CheckpointLoad { source: err })?;
        let storage = Arc::new(MVCCStorage::load::<EncodingKeyspace>(&name, path, wal, checkpoint)
            .map_err(|error| StorageOpen { source: error })?);
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone())
            .map_err(|err| Encoding { source: err })?);
        TypeManager::<WriteSnapshot<D>>::initialise_types(storage.clone(), type_vertex_generator.clone())
            .map_err(|err| SchemaInitialise { source: err })?;
        // let statistics = Statistics::new(storage.);

        Ok(Database::<WAL> {
            name: name.as_ref().to_owned(),
            path: path.to_owned(),
            storage,
            type_vertex_generator,
            thing_vertex_generator,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub enum DatabaseOpenError {
    InvalidUnicodeName { name: OsString },
    DirectoryCreate { path: PathBuf, source: io::Error },
    StorageOpen { source: StorageOpenError },
    DurabilityOpen { source: DurabilityError },
    CheckpointLoad { source: CheckpointLoadError },
    Encoding { source: EncodingError },
    SchemaInitialise { source: ConceptWriteError },
}

impl fmt::Display for DatabaseOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUnicodeName { .. } => None,
            Self::DirectoryCreate { source, .. } => Some(source),
            Self::StorageOpen { source } => Some(source),
            Self::DurabilityOpen{ source } => Some(source),
            Self::CheckpointLoad{ source } => Some(source),
            Self::SchemaInitialise { source } => Some(source),
            Self::Encoding { source } => Some(source),
        }
    }
}
