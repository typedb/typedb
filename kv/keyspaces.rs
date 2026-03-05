/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, path::Path, sync::Arc};

use error::{typedb_error, TypeDBError};
use serde::{Deserialize, Serialize};

use crate::{write_batches::WriteBatches, KVStore, KVStoreID};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyspaceId(pub u8);

impl fmt::Debug for KeyspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for KeyspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<KeyspaceId> for KVStoreID {
    fn from(value: KeyspaceId) -> KVStoreID {
        value.0 as KVStoreID
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: adjusting these constants affects many things, including serialised WAL records and in-memory data structures.  //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
pub const KEYSPACE_MAXIMUM_COUNT: usize = 10;
pub(crate) const KEYSPACE_ID_MAX: KeyspaceId = KeyspaceId(KEYSPACE_MAXIMUM_COUNT as u8 - 1);
pub(crate) const KEYSPACE_ID_RESERVED_UNSET: KeyspaceId = KeyspaceId(KEYSPACE_ID_MAX.0 + 1);

pub trait KeyspaceSet: Copy {
    fn iter() -> impl Iterator<Item = Self>;
    fn id(&self) -> KeyspaceId;
    fn name(&self) -> &'static str;
    fn prefix_length(&self) -> Option<usize>;
}

#[derive(Debug)]
pub struct Keyspaces {
    pub(crate) keyspaces: Vec<KVStore>,
    pub(crate) index: [Option<KeyspaceId>; KEYSPACE_MAXIMUM_COUNT],
}

impl Keyspaces {
    pub(crate) fn new() -> Self {
        Self { keyspaces: Vec::new(), index: std::array::from_fn(|_| None) }
    }

    pub(crate) fn validate_new_keyspace(&self, keyspace_id: impl KeyspaceSet) -> Result<(), KeyspacesError> {
        use KeyspacesError::{IdExists, IdReserved, IdTooLarge, NameExists};

        let name = keyspace_id.name();

        if keyspace_id.id() == KEYSPACE_ID_RESERVED_UNSET {
            return Err(IdReserved { name, id: keyspace_id.id().0 });
        }

        if keyspace_id.id() > KEYSPACE_ID_MAX {
            return Err(IdTooLarge { name, id: keyspace_id.id().0, max_id: KEYSPACE_ID_MAX.0 });
        }

        for (existing_id, existing_keyspace_index) in self.index.iter().enumerate() {
            if let Some(existing_index) = existing_keyspace_index {
                let keyspace = &self.keyspaces[existing_index.0 as usize];
                if keyspace.name() == name {
                    return Err(NameExists { name });
                }
                if existing_id == keyspace_id.id().0 as usize {
                    return Err(IdExists { new_name: name, id: keyspace_id.id().0, existing_name: keyspace.name() });
                }
            }
        }
        Ok(())
    }

    pub fn get(&self, keyspace_id: KeyspaceId) -> &KVStore {
        let keyspace_index = self.index[keyspace_id.0 as usize].unwrap();
        &self.keyspaces[keyspace_index.0 as usize]
    }

    pub fn write(&self, write_batches: WriteBatches) -> Result<(), KeyspacesError> {
        for (index, write_batch) in write_batches.into_iter() {
            debug_assert!(index < KEYSPACE_MAXIMUM_COUNT);
            self.get(KeyspaceId(index as u8))
                .write(write_batch)
                .map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
        }
        Ok(())
    }

    pub fn checkpoint(&self, current_checkpoint_dir: &Path) -> Result<(), KeyspacesError> {
        for keyspace in &self.keyspaces {
            keyspace
                .checkpoint(current_checkpoint_dir)
                .map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
        }
        Ok(())
    }

    pub fn delete(self) -> Result<(), Vec<KeyspacesError>> {
        let mut errors = Vec::new();
        for keyspace in self.keyspaces {
            if let Err(e) = keyspace.delete() {
                errors.push(KeyspacesError::KVStoreError { typedb_source: e.into() });
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), KeyspacesError> {
        for keyspace in self.keyspaces.iter_mut() {
            keyspace.reset().map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
        }
        Ok(())
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, KeyspacesError> {
        self.keyspaces.iter().try_fold(0, |total, keyspace| {
            let size = keyspace
                .estimate_size_in_bytes()
                .map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
            Ok(total + size)
        })
    }

    pub fn estimate_key_count(&self) -> Result<u64, KeyspacesError> {
        self.keyspaces.iter().try_fold(0, |total, keyspace| {
            let count =
                keyspace.estimate_key_count().map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
            Ok(total + count)
        })
    }
}

typedb_error!(
    pub KeyspacesError(component = "Keyspaces error", prefix = "KSE") {
        KVStoreError(1, "KV store error.", typedb_source: Arc<dyn TypeDBError>),
        IdReserved(2, "Keyspace ID {id} is reserved and cannot be used for new keyspace '{name}'.", name: &'static str, id: u8),
        IdTooLarge(3, "Keyspace ID is too large. Name: {name}, id: {id} > max allowed id: {max_id}.", name: &'static str, id: u8, max_id: u8 ),
        NameExists(4, "Keyspace name '{name}' exists and cannot be created again.", name: &'static str),
        IdExists(5, "Keyspace ID {id} cannot be used for new keyspace '{new_name}' since it is already used by keyspace '{existing_name}'.",new_name: &'static str, id: u8, existing_name: &'static str),
    }
);
