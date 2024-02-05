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

pub type KeyspaceId = u8;

pub(crate) const KEYSPACE_ID_MAX: usize = KeyspaceId::MAX as usize;


///
/// A non-durable key-value store that supports put, get, delete, iterate
/// and checkpointing.
///
pub(crate) struct Keyspace {
    path: PathBuf,
    kv_storage: DB,
    next_checkpoint_id: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
}

impl Keyspace {
    pub(crate) fn new(path: PathBuf, options: &Options) -> Result<Keyspace, KeyspaceError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| KeyspaceError {
                kind: KeyspaceErrorKind::FailedKeyspaceCreate { source: e },
            })?;
        // initial read options, should be customised to this storage's properties
        let read_options = ReadOptions::default();
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        Ok(Keyspace {
            path: path,
            kv_storage: kv_storage,
            next_checkpoint_id: 0,
            read_options: read_options,
            write_options: write_options,
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

    fn new_read_options(&self) -> ReadOptions {
        ReadOptions::default()
    }

    fn new_write_options(&self) -> WriteOptions {
        let mut options = WriteOptions::default();
        options.disable_wal(true);
        options
    }

    pub(crate) fn name(&self) -> &str {
        self.path.file_name().unwrap().to_str().unwrap()
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KeyspaceError> {
        self.kv_storage.put_opt(key, value, &self.write_options)
            .map_err(|e| KeyspaceError {
                kind: KeyspaceErrorKind::FailedPut { source: e },
            })
    }

    pub(crate) fn get<M, V>(&self, key: &[u8], mut mapper: M) -> Result<Option<V>, KeyspaceError>
        where M: FnMut(&[u8]) -> V {
        self.kv_storage.get_pinned_opt(key, &self.read_options)
            .map(|option| option.map(|value|
                mapper(value.as_ref())
            )).map_err(|err| KeyspaceError {
            kind: KeyspaceErrorKind::FailedGet { source: err }
        })
    }

    pub(crate) fn get_prev<M, K, V>(&self, key: &[u8], mut mapper: M) -> Option<(K, V)>
        where M: FnMut(&[u8], &[u8]) -> (K, V) {
        let mut iterator = self.kv_storage.raw_iterator_opt(self.new_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| mapper(k, v))
    }

    // TODO: we should benchmark using iterator pools, which would require changing prefix/range on read options
    pub(crate) fn iterate_prefix<'s, C: 's, K: 's, V: 's>(&'s self, prefix: &'s [u8], mut controller: C) -> impl Iterator<Item=Result<(K, V), KeyspaceError>> + 's
        where C: IteratorController<K, V> {
        KeyspacePrefixIterator::new(&self, prefix, controller)
    }

    pub(crate) fn write(&self, write_batch: WriteBatch) -> Result<(), KeyspaceError> {
        self.kv_storage.write_opt(write_batch, &self.write_options).map_err(|error| {
            KeyspaceError {
                kind: KeyspaceErrorKind::FailedBatchWrite { source: error },
            }
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<(), KeyspaceError> {
        todo!()
        // Steps:
        //  Create new checkpoint directory at 'checkpoint_{next_checkpoint_id}'
        //  Take the last sequence number watermark
        //  Take a storage checkpoint into directory (may end up containing some more commits, which is OK)
        //  Write properties file: timestamp and last sequence number watermark
    }

    pub(crate) fn delete(self) -> Result<(), KeyspaceError> {
        match std::fs::remove_dir_all(self.path.clone()) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(KeyspaceError {
                    kind: KeyspaceErrorKind::FailedKeyspaceDelete { source: e },
                })
            }
        }
    }
}

pub(crate) struct KeyspacePrefixIterator<'s, C, K, V>
    where C: IteratorController<K, V> {
    prefix: &'s [u8],
    iterator: DBRawIterator<'s>,
    controller: C,
    key: PhantomData<K>,
    value: PhantomData<V>,
    done: bool,
}

impl<'s, C, K, V> KeyspacePrefixIterator<'s, C, K, V>
    where C: IteratorController<K, V> {
    fn new(keyspace: &'s Keyspace, prefix: &'s [u8], controller: C) -> KeyspacePrefixIterator<'s, C, K, V> {

        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);
        let mut read_opts = keyspace.new_read_options();
        let mut raw_iterator: DBRawIteratorWithThreadMode<'s, DB> = keyspace.kv_storage.raw_iterator_opt(read_opts);
        raw_iterator.seek(prefix);

        KeyspacePrefixIterator {
            prefix: prefix,
            iterator: raw_iterator,
            controller: controller,
            key: PhantomData::default(),
            value: PhantomData::default(),
            done: false,
        }
    }

    fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }
}

impl<'s, C, K, V> Iterator for KeyspacePrefixIterator<'s, C, K, V>
    where C: IteratorController<K, V> {
    type Item = Result<(K, V), KeyspaceError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            } else if !self.iterator.valid() {
                self.done = true;
                return None;
            } else if self.iterator.status().is_err() {
                self.done = true;
                return Some(Err(KeyspaceError {
                    kind: KeyspaceErrorKind::FailedIterate {
                        source: self.iterator.status().err().unwrap()
                    }
                }));
            } else {
                let key = self.iterator.key().unwrap();
                if key.len() < self.prefix.len() || &key[0..self.prefix.len()] != self.prefix {
                    self.done = true;
                    return None;
                }

                let value = self.iterator.value().unwrap();
                let control = self.controller.control(key, value);
                match control {
                    IterControl::IgnoreSingle => continue,
                    IterControl::IgnoreUntil(k) => {
                        self.iterator.seek(k);
                        continue;
                    }
                    IterControl::Accept(k, v) => return Some(Ok::<(K, V), KeyspaceError>((k, v))),
                    IterControl::Stop => {
                        self.done = true;
                        return None;
                    }
                }
            }
        }
    }
}

pub(crate) trait IteratorController<K, V> {

    fn control<'a>(&'a mut self, key: &[u8], value: &[u8]) -> IterControl<'a, K, V>;

}

pub(crate) enum IterControl<'a, K, V> {
    IgnoreSingle,
    IgnoreUntil(&'a [u8]),
    Accept(K, V),
    Stop,
}

impl<'a, K, V> IterControl<'a, K, V> {
    pub(crate) fn map<F, T, U>(self, mut func: F) -> IterControl<'a, T, U>
        where F: FnMut(K, V) -> (T, U) {
        match self {
            IterControl::IgnoreSingle => IterControl::IgnoreSingle,
            IterControl::IgnoreUntil(k) => IterControl::IgnoreUntil(k),
            IterControl::Accept(k, v) => {
                let (t, u) = func(k, v);
                IterControl::Accept(t, u)
            }
            IterControl::Stop => IterControl::Stop
        }
    }

    const fn is_ignore_single(&self) -> bool {
        matches!(self, IterControl::IgnoreSingle)
    }

    const fn is_accept(&self) -> bool {
        matches!(self, IterControl::Accept(_, _))
    }

    const fn is_stop(&self) -> bool {
        matches!(self, IterControl::Stop)
    }

    fn try_into_ignore_until(self) -> Option<&'a [u8]> {
        if let IterControl::IgnoreUntil(bytes) = self {
            Some(bytes)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct KeyspaceError {
    pub kind: KeyspaceErrorKind,
}

#[derive(Debug)]
pub enum KeyspaceErrorKind {
    FailedKeyspaceCreate { source: speedb::Error },
    FailedKeyspaceDelete { source: std::io::Error },
    FailedGet { source: speedb::Error },
    FailedPut { source: speedb::Error },
    FailedBatchWrite { source: speedb::Error },
    FailedIterate { source: speedb::Error },
}

impl Display for KeyspaceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for KeyspaceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            KeyspaceErrorKind::FailedKeyspaceDelete { source, .. } => Some(source),
            KeyspaceErrorKind::FailedGet { source, .. } => Some(source),
            KeyspaceErrorKind::FailedPut { source, .. } => Some(source),
            KeyspaceErrorKind::FailedBatchWrite { source, .. } => Some(source),
            KeyspaceErrorKind::FailedIterate { source, .. } => Some(source),
            KeyspaceErrorKind::FailedKeyspaceCreate { source } => Some(source),
        }
    }
}


