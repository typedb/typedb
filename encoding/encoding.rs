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

use std::ops::Range;

use bytes::{byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use storage::{error::MVCCStorageError, key_value::StorageKey, MVCCStorage};

use crate::layout::prefix::{PrefixID, PrefixType};

mod error;
pub mod graph;
pub mod layout;
pub mod value;

pub enum EncodingKeyspace {
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

    pub const fn id(&self) -> KeyspaceId {
        match self {
            EncodingKeyspace::Schema => 0x0,
            EncodingKeyspace::Data => 0x1,
        }
    }

    fn setup_storage(&self, storage: &mut MVCCStorage) -> Result<(), MVCCStorageError> {
        let options = MVCCStorage::new_db_options();
        storage.create_keyspace(self.name(), self.id(), &options)
    }
}

pub fn create_keyspaces(storage: &mut MVCCStorage) -> Result<(), MVCCStorageError> {
    EncodingKeyspace::Schema.setup_storage(storage)?;
    EncodingKeyspace::Data.setup_storage(storage)
}

pub trait AsBytes<'a, const INLINE_SIZE: usize> {
    fn bytes(&'a self) -> ByteReference<'a>;

    fn into_bytes(self) -> ByteArrayOrRef<'a, INLINE_SIZE>;
}

pub trait Keyable<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> + Sized {
    fn keyspace_id(&self) -> KeyspaceId;

    fn as_storage_key(&'a self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new_ref(self.keyspace_id(), self.bytes())
    }

    fn into_storage_key(self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new(self.keyspace_id(), self.into_bytes())
    }
}

pub trait Prefixed<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> {
    const RANGE_PREFIX: Range<usize> = 0..PrefixID::LENGTH;

    fn prefix(&'a self) -> PrefixType {
        let bytes = &self.bytes().bytes()[Self::RANGE_PREFIX].try_into().unwrap();
        PrefixType::from_prefix_id(PrefixID::new(*bytes))
    }
}
