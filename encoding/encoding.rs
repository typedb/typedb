/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::ops::Range;

use bytes::{byte_reference::ByteReference, Bytes};
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
    /// Keyspace optimised for 11 byte prefix seeks:
    /// mostly Short Attribute Vertices that are Put (12 bytes)
    /// also existence/IID checks for Object vertices (11 bytes)
    /// also schema (default KS)
    /// Ordered Object Properties prefix: [1: prefix][11: object][1: ordered property] (13 bytes)
    DefaultOptimisedPrefix11,

    /// Keyspace optimised for 15 byte prefix seeks:
    /// Links & Links Reverse prefix:  [1: prefix][11: from][3: to type]
    /// Has prefix:  [1: prefix][11: from][3: to type]
    OptimisedPrefix15,

    /// Keyspace optimised for 16 byte prefix seeks:
    /// Has Reverse prefix for Short attribute vertices: [1: prefix][12: from][3: to type]
    OptimisedPrefix16,

    /// Keyspace optimised for 17 byte prefix seeks:
    /// Long Attribute Vertices existence checks have 21 bytes [1: prefix][2: type][18: ID + category], but could still benefit from a 17 byte bloom prefix
    /// LinksIndex prefix: [1: prefix][11: player 1][2: rel type id][3: player 2 type]
    OptimisedPrefix17,


    /// Keyspace optimised for 25 byte prefix seeks:
    /// Has Reverse prefix for Long attribute vertices: [1: prefix][21: from][3: to type]
    OptimisedPrefix25,
}

impl KeyspaceSet for EncodingKeyspace {
    fn iter() -> impl Iterator<Item = Self> {
        [
            Self::DefaultOptimisedPrefix11,
            Self::OptimisedPrefix15,
            Self::OptimisedPrefix16,
            Self::OptimisedPrefix17,
            Self::OptimisedPrefix25,
        ].into_iter()
    }

    fn id(&self) -> KeyspaceId {
        match self {
            EncodingKeyspace::DefaultOptimisedPrefix11 => KeyspaceId(0x0),
            EncodingKeyspace::OptimisedPrefix15 => KeyspaceId(0x1),
            EncodingKeyspace::OptimisedPrefix16 => KeyspaceId(0x2),
            EncodingKeyspace::OptimisedPrefix17 => KeyspaceId(0x3),
            EncodingKeyspace::OptimisedPrefix25 => KeyspaceId(0x4),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            EncodingKeyspace::DefaultOptimisedPrefix11 => "OptimisedPrefix11",
            EncodingKeyspace::OptimisedPrefix15 => "OptimisedPrefix15",
            EncodingKeyspace::OptimisedPrefix16 => "OptimisedPrefix16",
            EncodingKeyspace::OptimisedPrefix17 => "OptimisedPrefix17",
            EncodingKeyspace::OptimisedPrefix25 => "OptimisedPrefix25",
        }
    }
}

pub trait AsBytes<'a, const INLINE_SIZE: usize> {
    fn bytes(&'a self) -> ByteReference<'a>;

    fn into_bytes(self) -> Bytes<'a, INLINE_SIZE>;
}

pub trait Keyable<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> + Sized {
    fn keyspace(&self) -> EncodingKeyspace;

    fn as_storage_key(&'a self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new_ref(self.keyspace(), self.bytes())
    }

    fn into_storage_key(self) -> StorageKey<'a, INLINE_SIZE> {
        StorageKey::new(self.keyspace(), self.into_bytes())
    }
}

pub trait Prefixed<'a, const INLINE_SIZE: usize>: AsBytes<'a, INLINE_SIZE> {
    const RANGE_PREFIX: Range<usize> = 0..PrefixID::LENGTH;

    fn prefix(&'a self) -> Prefix {
        let bytes = &self.bytes().bytes()[Self::RANGE_PREFIX].try_into().unwrap();
        Prefix::from_prefix_id(PrefixID::new(*bytes))
    }
}
