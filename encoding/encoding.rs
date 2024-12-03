/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(elided_lifetimes_in_paths)]
#![deny(unused_must_use)]

use bytes::Bytes;
use storage::{
    key_value::StorageKey,
    keyspace::{KeyspaceId, KeyspaceSet},
};

use crate::layout::prefix::{Prefix, PrefixID};

pub mod error;
pub mod graph;
pub mod layout;
pub mod value;

/*
 * TODO: things we may want to allow the user to configure, per database:
 * - Bytes per TypeID (max number of types per kind)
 * - Bytes per Entity/Relation (ObjectID) (max number of instances per type)
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

    fn id(&self) -> KeyspaceId {
        match self {
            Self::Schema => KeyspaceId(0x0),
            Self::Data => KeyspaceId(0x1),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Data => "data",
        }
    }
}

pub trait AsBytes<const INLINE_SIZE: usize> {
    fn to_bytes(self) -> Bytes<'static, INLINE_SIZE>;
}

pub trait Keyable<const INLINE_SIZE: usize>: AsBytes<INLINE_SIZE> + Sized {
    fn keyspace(&self) -> EncodingKeyspace;

    fn into_storage_key(self) -> StorageKey<'static, INLINE_SIZE> {
        StorageKey::new(self.keyspace(), self.to_bytes())
    }
}

pub trait Prefixed<const INLINE_SIZE: usize>: AsBytes<INLINE_SIZE> + Clone {
    const INDEX_PREFIX: usize = 0;

    fn prefix(&self) -> Prefix {
        let byte = self.clone().to_bytes()[Self::INDEX_PREFIX];
        Prefix::from_prefix_id(PrefixID::new(byte))
    }
}
