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


use storage::error::MVCCStorageError;
use storage::keyspace::keyspace::KeyspaceId;
use storage::MVCCStorage;

pub mod graph;
pub mod layout;
mod serializable;
mod error;
pub mod primitive;

enum EncodingKeyspace {
    Schema,
    Data, // TODO: partition into sub-keyspaces for write optimisation
}

impl EncodingKeyspace {
    const fn name(&self) -> &str {
        match self {
            EncodingKeyspace::Schema => "schema",
            EncodingKeyspace::Data => "data",
        }
    }

    const fn id(&self) -> KeyspaceId {
        match self {
            EncodingKeyspace::Schema => 0x0,
            EncodingKeyspace::Data => 0x1,
        }
    }

    fn initialise_storage(&self, storage: &mut MVCCStorage) -> Result<(), MVCCStorageError> {
        let options = MVCCStorage::new_db_options();
        storage.create_keyspace(self.name(), self.id(), &options)
    }
}

pub fn initialise_storage(storage: &mut MVCCStorage) -> Result<(), MVCCStorageError> {
    EncodingKeyspace::Schema.initialise_storage(storage)?;
    EncodingKeyspace::Data.initialise_storage(storage)
}
