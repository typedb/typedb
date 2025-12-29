/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod iterator;
mod rocks;

use crate::iterator::KVStoreRangeIterator;
use bytes::Bytes;
use error::TypeDBError;
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;
use std::path::{Path, PathBuf};

pub trait KVStore {
    type SharedResources;
    type OpenOptions;
    type RangeIterator: KVStoreRangeIterator;
    type WriteBatch: KVWriteBatch;

    fn create_shared_resources() -> Self::SharedResources;

    fn create_open_options(
        shared_resources: &Self::SharedResources,
        path_buf: PathBuf,
        prefix_length: Option<usize>,
    ) -> Self::OpenOptions;

    fn open<'a>(
        options: &Self::OpenOptions,
        name: &'static str,
        id: KVStoreID,
    ) -> Result<Self, Box<dyn KVStoreError>>
    where
        Self: Sized;

    fn id(&self) -> KVStoreID;

    fn name(&self) -> &'static str;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn KVStoreError>>;

    fn get<M, V>(&self, key: &[u8], mapper: &mut M) -> Result<Option<V>, Box<dyn KVStoreError>>
    where
        M: FnMut(&[u8]) -> V;

    fn get_prev<M, T>(&self, key: &[u8], mapper: &mut M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T;

    fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> Self::RangeIterator;

    fn write(&self, write_batch: Self::WriteBatch) -> Result<(), Box<dyn KVStoreError>>;

    fn checkpoint(&self, checkpoint_dir: &Path) -> Result<(), Box<dyn KVStoreError>>;

    fn delete(self) -> Result<(), Box<dyn KVStoreError>>;

    fn reset(&mut self) -> Result<(), Box<dyn KVStoreError>>;

    fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn KVStoreError>>;

    fn estimate_key_count(&self) -> Result<u64, Box<dyn KVStoreError>>;
}

pub trait KVStoreError: TypeDBError + Send + Sync {}

impl<T: KVStoreError + 'static> From<T> for Box<dyn KVStoreError> {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

pub trait KVWriteBatch: Default {
    fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>);
}

pub type KVStoreID = usize;
