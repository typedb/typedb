/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(elided_lifetimes_in_paths)]
#![deny(unused_must_use)]

use bytes::{util::MB, Bytes};
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, DBCompressionType, SliceTransform};
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
        ]
        .into_iter()
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

    fn rocks_configuration(&self, cache: &rocksdb::Cache) -> rocksdb::Options {
        let mut options = rocksdb::Options::default();

        // Enable if we wanted to check bloom filter usage, cache hits, etc.
        // options.enable_statistics();
        // options.set_stats_dump_period_sec(100);
        // options.set_statistics_level(StatsLevel::All);

        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_background_jobs(10);
        options.set_target_file_size_base(64 * MB);
        options.set_write_buffer_size(64 * MB as usize);
        options.set_max_write_buffer_size_to_maintain(0);
        options.set_max_write_buffer_number(2);
        options.set_memtable_whole_key_filtering(false);
        options.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
        ]);

        // TODO: 2.x has   enable_index_compression: 1 set to 0

        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_cache(&cache);
        block_options.set_block_restart_interval(16);
        block_options.set_index_block_restart_interval(16);
        block_options.set_format_version(6);
        block_options.set_block_size(16 * 1024);
        block_options.set_whole_key_filtering(false);

        block_options.set_bloom_filter(10.0, false);
        block_options.set_partition_filters(true);
        block_options.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        block_options.set_optimize_filters_for_memory(true);
        block_options.set_pin_top_level_index_and_filter(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_cache_index_and_filter_blocks(true);

        match self {
            EncodingKeyspace::DefaultOptimisedPrefix11 => {
                options.set_prefix_extractor(SliceTransform::create_fixed_prefix(11))
            }
            EncodingKeyspace::OptimisedPrefix15 => {
                options.set_prefix_extractor(SliceTransform::create_fixed_prefix(15))
            }
            EncodingKeyspace::OptimisedPrefix16 => {
                options.set_prefix_extractor(SliceTransform::create_fixed_prefix(16))
            }
            EncodingKeyspace::OptimisedPrefix17 => {
                options.set_prefix_extractor(SliceTransform::create_fixed_prefix(17))
            }
            EncodingKeyspace::OptimisedPrefix25 => {
                options.set_prefix_extractor(SliceTransform::create_fixed_prefix(25))
            }
        }

        options.set_block_based_table_factory(&block_options);
        options
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
