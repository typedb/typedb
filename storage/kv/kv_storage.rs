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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::path::PathBuf;

use speedb::{DB, DBRawIterator, DBRawIteratorWithThreadMode, Options, ReadOptions, WriteBatch, WriteOptions};

///
/// A non-durable key-value store that supports put, get, delete, iterate
/// and checkpointing.
///
pub(crate) struct KVStorage {
    path: PathBuf,
    kv_storage: DB,
    next_checkpoint_id: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
}

impl KVStorage {
    pub(crate) fn new(path: PathBuf, options: &Options) -> Result<KVStorage, KVStorageError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| KVStorageError {
                kind: KVStorageErrorKind::FailedStorageCreate { source: e },
            })?;

        Ok(KVStorage {
            path: path,
            kv_storage: kv_storage,
            next_checkpoint_id: 0,
            read_options: KVStorage::new_read_options(),
            write_options: KVStorage::new_write_options(),
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    pub(crate) fn new_from_checkpoint(path: PathBuf) {
        todo!()
        // Steps:
        //  WARNING: this is intended to be DESTRUCTIVE since we may wipe anything partially written in the active directory
        //  Locate the directory with the latest number - say 'checkpoint_n'
        //  Delete 'active' directory.
        //  Rename directory called 'active' to 'checkpoint_x' -- TODO: do we need to delete 'active' or will re-checkpointing to it be clever enough to delete corrupt files?
        //  Rename 'checkpoint_x' to 'active'
        //  open storage at 'active'
    }

    fn new_read_options() -> ReadOptions {
        ReadOptions::default()
    }

    fn new_write_options() -> WriteOptions {
        let mut options = WriteOptions::default();
        options.disable_wal(true);
        options
    }

    pub(crate) fn name(&self) -> &str {
        self.path.file_name().unwrap().to_str().unwrap()
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KVStorageError> {
        self.kv_storage.put_opt(key, value, &self.write_options)
            .map_err(|e| KVStorageError {
                kind: KVStorageErrorKind::FailedPut { source: e },
            })
    }

    pub(crate) fn get<R, V>(&self, key: &[u8], mut reader: R) -> Result<Option<V>, KVStorageError>
        where R: FnMut(&[u8]) -> V {
        self.kv_storage.get_pinned_opt(key, &self.read_options)
            .map(|option| option.map(|value|
                reader(value.as_ref())
            )).map_err(|err| KVStorageError {
            kind: KVStorageErrorKind::FailedGet { source: err }
        })
    }

    pub(crate) fn get_prev<R, K, V>(&self, key: &[u8], mut reader: R) -> Option<(K, V)>
        where R: FnMut(&[u8], &[u8]) -> (K, V) {
        let mut iterator = self.kv_storage.raw_iterator_opt(KVStorage::new_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| reader(k, v))
    }

    // TODO: we should benchmark using iterator pools
    pub(crate) fn iterate_prefix<'s, R: 's, K: 's, V: 's>(&'s self, prefix: &[u8], reader: R) -> impl Iterator<Item=Result<(K, V), KVStorageError>> + 's
        where R: FnMut(&[u8], &[u8]) -> Option<(K, V)> {
        let raw_iterator: DBRawIteratorWithThreadMode<'s, DB> = self.kv_storage.raw_iterator_opt(KVStorage::new_read_options());
        let mut kv_iterator = KVIterator::new(raw_iterator, reader);
        kv_iterator.seek(prefix);
        kv_iterator
    }

    pub(crate) fn write(&self, write_batch: WriteBatch) -> Result<(), KVStorageError> {
        self.kv_storage.write_opt(write_batch, &self.write_options).map_err(|error| {
            KVStorageError {
                kind: KVStorageErrorKind::FailedBatchWrite { source: error },
            }
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<(), KVStorageError> {
        todo!()
        // Steps:
        //  Create new checkpoint directory at 'checkpoint_{next_checkpoint_id}'
        //  Take the last sequence number watermark
        //  Take a storage checkpoint into directory (may end up containing some more commits, which is OK)
        //  Write properties file: timestamp and last sequence number watermark
    }

    pub(crate) fn delete(self) -> Result<(), KVStorageError> {
        match std::fs::remove_dir_all(self.path.clone()) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(KVStorageError {
                    kind: KVStorageErrorKind::FailedStorageDelete { source: e },
                })
            }
        }
    }
}

pub(crate) struct KVIterator<'a, R, K, V> {
    iterator: DBRawIterator<'a>,
    reader: R,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<'a, R, K, V> KVIterator<'a, R, K, V> where R: FnMut(&[u8], &[u8]) -> Option<(K, V)> {
    fn new(raw_iterator: DBRawIteratorWithThreadMode<'a, DB>, reader: R) -> KVIterator<'a, R, K, V> {
        KVIterator {
            iterator: raw_iterator,
            reader: reader,
            key: PhantomData::default(),
            value: PhantomData::default(),
        }
    }

    pub(crate) fn seek(&mut self, prefix: &[u8]) {
        self.iterator.seek(prefix)
    }
}

impl<'a, R, K, V> Iterator for KVIterator<'a, R, K, V> where R: FnMut(&[u8], &[u8]) -> Option<(K, V)> {
    type Item = Result<(K, V), KVStorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.iterator.status().is_err() {
                return Some(Err(KVStorageError {
                    kind: KVStorageErrorKind::FailedIterate {
                        source: self.iterator.status().err().unwrap()
                    }
                }));
            } else if !self.iterator.valid() {
                return None;
            } else {
                let (key, value) = self.iterator.item().unwrap();
                let read = (self.reader)(key, value);
                if read.is_some() {
                    return Some(Ok(read.unwrap()));
                } else {
                    continue;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct KVStorageError {
    pub kind: KVStorageErrorKind,
}

#[derive(Debug)]
pub enum KVStorageErrorKind {
    FailedStorageCreate { source: speedb::Error },
    FailedStorageDelete { source: std::io::Error },
    FailedGet { source: speedb::Error },
    FailedPut { source: speedb::Error },
    FailedBatchWrite { source: speedb::Error },
    FailedIterate { source: speedb::Error },
}

impl Display for KVStorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for KVStorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            KVStorageErrorKind::FailedStorageDelete { source, .. } => Some(source),
            KVStorageErrorKind::FailedGet { source, .. } => Some(source),
            KVStorageErrorKind::FailedPut { source, .. } => Some(source),
            KVStorageErrorKind::FailedBatchWrite { source, .. } => Some(source),
            KVStorageErrorKind::FailedIterate { source, .. } => Some(source),
            KVStorageErrorKind::FailedStorageCreate { source } => Some(source),
        }
    }
}


