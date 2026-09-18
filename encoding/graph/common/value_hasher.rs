/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use bytes::byte_array::ByteArray;
use lending_iterator::LendingIterator;
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::StorageCounters};
use storage::{
    key_range::KeyRange,
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, iterator::SnapshotIteratorError, write::Write},
};

use crate::{EncodingKeyspace, graph::common::ExistingOrNew};

pub(crate) trait HashedID<const DISAMBIGUATED_HASH_LENGTH: usize> {
    const HASH_LENGTH: usize = DISAMBIGUATED_HASH_LENGTH - 1;
    const HASH_DISAMBIGUATOR_BYTE_INDEX: usize = Self::HASH_LENGTH;
    const HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG: u8 = 0b1000_0000;
    const FIXED_WIDTH_KEYS: bool;

    // write the hash of the value to the start of the byte slice, and return the number of bytes written
    fn write_hash(bytes: &mut [u8], hasher: &impl Fn(&[u8]) -> u64, value_bytes: &[u8]) -> usize {
        debug_assert!(bytes.len() >= Self::HASH_LENGTH);
        let hash_bytes = &hasher(value_bytes).to_be_bytes()[0..Self::HASH_LENGTH];
        bytes[0..hash_bytes.len()].copy_from_slice(hash_bytes);
        Self::HASH_LENGTH
    }

    fn find_existing_or_next_disambiguated_hash<Snapshot>(
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        keyspace: EncodingKeyspace,
        key_without_hash: &[u8],
        value_bytes: &[u8],
    ) -> Result<ExistingOrNew<[u8; DISAMBIGUATED_HASH_LENGTH]>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        let mut key_without_tail_byte: ByteArray<BUFFER_KEY_INLINE> =
            ByteArray::zeros(key_without_hash.len() + Self::HASH_LENGTH);
        key_without_tail_byte[0..key_without_hash.len()].copy_from_slice(key_without_hash);
        let hash_bytes = Self::write_hash(
            &mut key_without_tail_byte[key_without_hash.len()..key_without_hash.len() + Self::HASH_LENGTH],
            hasher,
            value_bytes,
        );
        let hash_bytes = &key_without_tail_byte[key_without_hash.len()..key_without_hash.len() + hash_bytes];
        Ok(Self::disambiguate(snapshot, keyspace, &key_without_tail_byte, value_bytes)?
            .map(|tail| Self::concat_hash_and_tail(hash_bytes, tail)))
    }

    fn concat_hash_and_tail(hash_bytes: &[u8], tail: u8) -> [u8; DISAMBIGUATED_HASH_LENGTH] {
        let mut bytes: [u8; DISAMBIGUATED_HASH_LENGTH] = [0; DISAMBIGUATED_HASH_LENGTH];
        bytes[0..Self::HASH_LENGTH].copy_from_slice(hash_bytes);
        bytes[Self::HASH_LENGTH] = tail;
        bytes
    }

    fn disambiguate<Snapshot>(
        snapshot: &Snapshot,
        keyspace: EncodingKeyspace,
        key_without_tail_byte: &[u8],
        value_bytes: &[u8],
    ) -> Result<ExistingOrNew<u8>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        let tail_byte_index = key_without_tail_byte.len();
        let range = KeyRange::new_within(
            StorageKey::<BUFFER_KEY_INLINE>::new_ref(keyspace, key_without_tail_byte),
            Self::FIXED_WIDTH_KEYS,
        );
        // occupied tails, indexed by the tail with the hash flag stripped
        let mut occupied_tails = [false; 1 << 7];

        // search for visible/live keys in the 128 slots in the bucket
        let mut iter = snapshot.iterate_range(&range, StorageCounters::DISABLED);
        while let Some((key, value)) = iter.next().transpose()? {
            let key_tail = key.bytes()[tail_byte_index];
            if key_tail & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG == 0 {
                // an inlined ID that happens to share the hashed prefix bytes: not part of this bucket - at most one
                continue;
            }
            if &*value == value_bytes {
                // found existing matching value
                return Ok(ExistingOrNew::Existing(key_tail));
            }
            occupied_tails[(key_tail & !Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG) as usize] = true;
        }

        // Keys deleted in this transaction are hidden by the iteration above, but their tails must not be reused
        // within the same transaction: a concurrent transaction may hold the deleted vertex as unmodifiable,
        // and re-putting its key here would replace our buffered delete and hide the conflict.
        for (key, write) in snapshot.iterate_writes_range(&range) {
            if matches!(write, Write::Delete) {
                let key_tail = key.bytes()[tail_byte_index];
                if key_tail & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG != 0 {
                    occupied_tails[(key_tail & !Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG) as usize] = true;
                }
            }
        }

        match occupied_tails.iter().position(|occupied| !occupied) {
            Some(free_tail) => Ok(ExistingOrNew::New(Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG | free_tail as u8)),
            None => panic!("Too many hash collisions when allocating hash for prefix: {:?}", key_without_tail_byte),
        }
    }
}
