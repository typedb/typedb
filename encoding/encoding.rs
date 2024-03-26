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

use bytes::{Bytes, byte_reference::ByteReference};
use storage::{key_value::StorageKey, KeyspaceSet};

use crate::layout::prefix::{PrefixID, Prefix};

mod error;
pub mod graph;
pub mod layout;
pub mod value;

/*
TODO: things we may want to allow the user to configure, per database:
- Bytes per TypeID (max number of types per kind)
- Bytes per Entity/Relation (ObjectID) (max number of instances per type)
 */

#[derive(Clone, Copy, Debug)]
pub enum EncodingKeyspace {
    Schema,
    Data, // TODO: partition into sub-keyspaces for write optimisation

    // ThingVertex
    // ThingEdgeShortPrefix
    // ThingEdgeLongPrefix
    // Metadata -- statistics
}

impl KeyspaceSet for EncodingKeyspace {
    fn iter() -> impl Iterator<Item = Self> {
        [Self::Schema, Self::Data].into_iter()
    }

    fn id(&self) -> u8 {
        match self {
            Self::Schema => 0x0,
            Self::Data => 0x1,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Data => "data",
        }
    }
}

pub trait AsBytes<'a, const INLINE_SIZE: usize> {
    fn bytes(&'a self) -> ByteReference<'a>;

    fn into_bytes(self) -> Bytes<'a, INLINE_SIZE>;
}

pub trait Keyable<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> + Sized {
    const KEYSPACE_ID: EncodingKeyspace;

    fn as_storage_key(&'a self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new_ref(Self::KEYSPACE_ID, self.bytes())
    }

    fn into_storage_key(self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new(Self::KEYSPACE_ID, self.into_bytes())
    }
}

pub trait Prefixed<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> {
    const RANGE_PREFIX: Range<usize> = 0..PrefixID::LENGTH;

    fn prefix(&'a self) -> Prefix {
        let bytes = &self.bytes().bytes()[Self::RANGE_PREFIX].try_into().unwrap();
        Prefix::from_prefix_id(PrefixID::new(*bytes))
    }
}
