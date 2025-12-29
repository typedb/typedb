/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod iterator;
mod rocks;

use bytes::Bytes;
use error::TypeDBError;
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;
use std::path::PathBuf;

pub trait KVStore {
    type SharedResources;
    type OpenOptions;
    type RangeIterator;
    type WriteBatch;
    type CheckpointArgs<'a>;

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

    fn name(&self) -> &str;

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn KVStoreError>>;

    async fn get<M, V>(&self, key: &[u8], mapper: &mut M) -> Result<Option<V>, Box<dyn KVStoreError>>
    where
        M: FnMut(&[u8]) -> V;

    async fn get_prev<M, T>(&self, key: &[u8], mapper: &mut M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T;

    async fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> Self::RangeIterator;

    async fn write(&self, write_batch: Self::WriteBatch) -> Result<(), Box<dyn KVStoreError>>;

    async fn checkpoint<'a>(&self, args: &Self::CheckpointArgs<'a>) -> Result<(), Box<dyn KVStoreError>>;

    async fn delete(self) -> Result<(), Box<dyn KVStoreError>>;

    async fn reset(&mut self) -> Result<(), Box<dyn KVStoreError>>;

    fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn KVStoreError>>;

    fn estimate_key_count(&self) -> Result<u64, Box<dyn KVStoreError>>;
}

pub trait KVStoreError: TypeDBError {}

impl<T: KVStoreError + 'static> From<T> for Box<dyn KVStoreError> {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

pub type KVStoreID = usize;
