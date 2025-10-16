/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, error::Error, fmt, path::PathBuf};

use resource::internal_database_prefix;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{event, Level};
use uuid::Uuid;

pub const CACHE_DB_NAME_PREFIX: &str = concat!(internal_database_prefix!(), "cache-");

// A single-threaded configurable cache which prioritizes using a simple in-memory storage, but
// spills the excessive data not fitting into the memory requirements over to disk.
#[derive(Debug)]
pub struct SpilloverCache<T: Serialize + DeserializeOwned + Clone> {
    memory_storage: HashMap<String, T>,
    disk_storage_path: PathBuf,
    disk_storage: Option<rocksdb::DB>,
    memory_size_limit: usize,
}

impl<T: Serialize + DeserializeOwned + Clone> SpilloverCache<T> {
    pub fn new(disk_storage_dir: &PathBuf, name_prefix: Option<&str>, memory_size_limit: usize) -> Self {
        assert!(disk_storage_dir.is_dir(), "SpilloverCache requires a disk storage path to a directory!");
        let unique_db_name = Uuid::new_v4().to_string();
        let disk_storage_path =
            disk_storage_dir.join(format!("{}{}{}", CACHE_DB_NAME_PREFIX, name_prefix.unwrap_or(""), unique_db_name));

        SpilloverCache { memory_storage: HashMap::new(), disk_storage_path, disk_storage: None, memory_size_limit }
    }

    pub fn insert(&mut self, key: String, value: T) -> Result<(), CacheError> {
        self.remove(&key)?;
        match self.memory_storage.len() < self.memory_size_limit {
            true => {
                self.memory_storage.insert(key, value);
                Ok(())
            }
            false => self.disk_storage_insert(key, value),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<T>, CacheError> {
        match self.memory_storage.get(key).cloned() {
            Some(value) => Ok(Some(value)),
            None => self.disk_storage_get(key),
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<(), CacheError> {
        match self.memory_storage.remove(key) {
            Some(_) => Ok(()),
            None => self.disk_storage_remove(key),
        }
    }

    fn disk_storage_insert(&mut self, key: String, value: T) -> Result<(), CacheError> {
        if self.disk_storage.is_none() {
            let rocks_db = rocksdb::DB::open(&Self::rocks_configuration(), &self.disk_storage_path)
                .map_err(|source| CacheError::DiskStorageAccess { source })?;
            self.disk_storage = Some(rocks_db);
        }
        let serialized = bincode::serialize(&value).map_err(|_| CacheError::DiskStorageSerialization {})?;
        self.disk_storage
            .as_mut()
            .unwrap()
            .put(key, serialized)
            .map_err(|source| CacheError::DiskStorageAccess { source })
    }

    fn disk_storage_get(&self, key: &str) -> Result<Option<T>, CacheError> {
        if let Some(disk_storage) = &self.disk_storage {
            if let Some(bytes) = disk_storage.get(key).map_err(|source| CacheError::DiskStorageAccess { source })? {
                return bincode::deserialize(&bytes)
                    .map(|value| Some(value))
                    .map_err(|_| CacheError::DiskStorageDeserialization {});
            }
        }
        Ok(None)
    }

    fn disk_storage_remove(&mut self, key: &str) -> Result<(), CacheError> {
        match &mut self.disk_storage {
            Some(disk_storage) => disk_storage.delete(key).map_err(|source| CacheError::DiskStorageAccess { source }),
            None => Ok(()),
        }
    }

    fn rocks_configuration() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options
    }
}

impl<T: Serialize + DeserializeOwned + Clone> Drop for SpilloverCache<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.disk_storage)); // release its files
        if let Err(e) = std::fs::remove_dir_all(&self.disk_storage_path) {
            // Can be cleaned up by the cache's user
            event!(Level::TRACE, "Failed to delete a temporary DB directory {:?}: {e}", self.disk_storage_path);
        }
    }
}

#[derive(Clone, Debug)]
pub enum CacheError {
    DiskStorageAccess { source: rocksdb::Error },
    DiskStorageSerialization,
    DiskStorageDeserialization,
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::DiskStorageAccess { source } => write!(f, "Cannot access disk storage, {source}"),
            CacheError::DiskStorageSerialization => write!(f, "Internal error: cannot write data to the disk storage"),
            CacheError::DiskStorageDeserialization => {
                write!(f, "Internal error: disk storage is corrupted and data cannot be read")
            }
        }
    }
}

impl Error for CacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CacheError::DiskStorageAccess { source } => Some(source),
            CacheError::DiskStorageSerialization => None,
            CacheError::DiskStorageDeserialization => None,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use test_utils::{create_tmp_dir, TempDir};

    use crate::SpilloverCache;
    macro_rules! put {
        ($cache:ident, $key:literal, $value:literal) => {
            $cache.insert($key.to_owned(), $value.to_owned()).unwrap()
        };
    }
    macro_rules! get {
        ($cache:ident, $key:literal) => {
            $cache.get($key).unwrap().as_ref().map(String::as_str)
        };
    }

    fn create_cache_in_tmpdir() -> (TempDir, SpilloverCache<String>) {
        let tmp_dir = create_tmp_dir();
        let cache: SpilloverCache<String> = SpilloverCache::new(&tmp_dir.as_ref().to_path_buf(), Some("unit_test"), 1);
        (tmp_dir, cache)
    }

    #[test]
    fn test_insert_spillover_duplicates() {
        let (tmp_dir, mut cache) = create_cache_in_tmpdir();
        put!(cache, "key1", "value1");
        assert_eq!(get!(cache, "key1"), Some("value1"));
        put!(cache, "key1", "value2");
        assert_eq!(get!(cache, "key1"), Some("value2"));
    }

    #[test]
    fn test_delete_insert_duplicates() {
        let (tmp_dir, mut cache) = create_cache_in_tmpdir();
        put!(cache, "key1", "value1");
        assert_eq!(get!(cache, "key1"), Some("value1"));

        put!(cache, "key2", "value2_1");
        assert_eq!(cache.get("key2").unwrap().unwrap(), "value2_1");

        cache.remove("key1").unwrap();
        assert_eq!(get!(cache, "key1"), None);

        put!(cache, "key2", "value2_2");
        assert_eq!(get!(cache, "key2"), Some("value2_2"));

        cache.remove("key2").unwrap();
        assert_eq!(get!(cache, "key2"), None);
    }
}
