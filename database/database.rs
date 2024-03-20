/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::{
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::{
    create_keyspaces,
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
};
use storage::{snapshot::snapshot::Snapshot, MVCCStorage};

use crate::{
    error::{
        DatabaseError,
        DatabaseErrorKind::{FailedToCreateDirectory, FailedToCreateStorage, FailedToSetupStorage},
    },
    transaction::{TransactionRead, TransactionWrite},
};

pub struct Database {
    name: String,
    path: PathBuf,
    storage: MVCCStorage,
    type_vertex_generator: TypeVertexGenerator,
    thing_vertex_generator: ThingVertexGenerator,
}

impl Database {
    pub fn new(path: &Path, database_name: impl AsRef<str>) -> Result<Database, DatabaseError> {
        let database_path = path.with_extension(String::from(database_name.as_ref()));
        fs::create_dir(database_path.as_path()).map_err(|io_error| DatabaseError {
            database_name: database_name.to_string(),
            kind: FailedToCreateDirectory(io_error),
        })?;
        let mut storage = MVCCStorage::new(database_name, path).map_err(|storage_error| DatabaseError {
            database_name: database_name.to_string(),
            kind: FailedToCreateStorage(storage_error),
        })?;

        create_keyspaces(&mut storage).map_err(|storage_error| DatabaseError {
            database_name: database_name.to_string(),
            kind: FailedToSetupStorage(storage_error),
        })?;
        let type_vertex_generator = TypeVertexGenerator::new();
        let thing_vertex_generator = ThingVertexGenerator::new();
        TypeManager::initialise_types(&mut storage, &type_vertex_generator);

        let database = Database {
            name: database_name.clone(),
            path: database_path,
            storage,
            type_vertex_generator,
            thing_vertex_generator,
        };
        Ok(database)
    }

    pub fn transaction_read(&self) -> TransactionRead {
        let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Read(self.storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &self.type_vertex_generator, None)); // TODO pass cache
        let thing_manager = ThingManager::new(snapshot.clone(), &self.thing_vertex_generator, type_manager.clone());
        TransactionRead { snapshot, type_manager, thing_manager }
    }

    fn transaction_write(&self) -> TransactionWrite {
        let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Write(self.storage.open_snapshot_write()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &self.type_vertex_generator, None)); // TODO pass cache for data write txn
        let thing_manager = ThingManager::new(snapshot.clone(), &self.thing_vertex_generator, type_manager.clone());
        TransactionWrite { snapshot, type_manager, thing_manager }
    }
}
