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

use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;

use speedb::{DB, DBRawIterator, DBRawIteratorWithThreadMode, Options, ReadOptions, WriteBatch, WriteOptions};

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use iterator::State;
use logger::result::ResultExt;

pub type KeyspaceId = u8;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: adjusting these constants affects many things, including serialised WAL records and in-memory data structures.  //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
pub(crate) const KEYSPACE_MAXIMUM_COUNT: usize = 10;
pub(crate) const KEYSPACE_ID_MAX: KeyspaceId = KEYSPACE_MAXIMUM_COUNT as u8 - 1;
pub(crate) const KEYSPACE_ID_RESERVED_UNSET: KeyspaceId = KEYSPACE_ID_MAX + 1;


///
/// A non-durable key-value store that supports put, get, delete, iterate
/// and checkpointing.
///
pub(crate) struct Keyspace {
    path: PathBuf,
    kv_storage: DB,
    keyspace_id: KeyspaceId,
    next_checkpoint_id: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
}

impl Keyspace {
    pub(crate) fn new(path: PathBuf, options: &Options, id: KeyspaceId) -> Result<Keyspace, KeyspaceError> {
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
            keyspace_id: id,
            next_checkpoint_id: 0,
            read_options: read_options,
            write_options: write_options,
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    pub(crate) fn load_from_checkpoint(path: PathBuf) {
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

    pub(crate) fn id(&self) -> KeyspaceId {
        self.keyspace_id
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

    pub(crate) fn get_prev<M,T>(&self, key: &[u8], mut mapper: M) -> Option<T>
        where M: FnMut(&[u8], &[u8]) -> T {
        let mut iterator = self.kv_storage.raw_iterator_opt(self.new_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| mapper(k, v))
    }

    // TODO: we should benchmark using iterator pools, which would require changing prefix/range on read options
    pub(crate) fn iterate_prefix<'s, const PREFIX_INLINE_SIZE: usize>(&'s self, prefix: ByteArrayOrRef<'s, PREFIX_INLINE_SIZE>) -> KeyspacePrefixIterator<'s, PREFIX_INLINE_SIZE> {
        KeyspacePrefixIterator::new(&self, prefix)
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

impl Debug for Keyspace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Keyspace[name={}, path={}, id={}, next_checkpoint_id={}]",
               self.name(), self.path.to_str().unwrap(), self.keyspace_id, self.next_checkpoint_id
        )
    }
}

pub struct KeyspacePrefixIterator<'a, const PS: usize> {
    prefix:  ByteArrayOrRef<'a, PS>,
    iterator: DBRawIterator<'a>,
    state: State<speedb::Error>,
}

impl<'a, const PS: usize> KeyspacePrefixIterator<'a, PS> {

    fn new(keyspace: &'a Keyspace, prefix:  ByteArrayOrRef<'a, PS>) -> Self {
        // TODO: if self.has_prefix_extractor_for(prefix), we can enable bloom filters
        // read_opts.set_prefix_same_as_start(true);
        let mut read_opts = keyspace.new_read_options();
        let raw_iterator: DBRawIteratorWithThreadMode<'a, DB> = keyspace.kv_storage.raw_iterator_opt(read_opts);

        KeyspacePrefixIterator {
            prefix: prefix,
            iterator: raw_iterator,
            state: State::Init,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match &self.state {
            State::Init => {
                self.iterator.seek(self.prefix.bytes());
                self.update_state();
                self.peek()
            }
            State::ItemReady => {
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.peek()
            }
            State::Error(error) => Some(Err(KeyspaceError {
                kind: KeyspaceErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<(&[u8], &[u8]), KeyspaceError>> {
        match &self.state {
            State::Init => {
                self.iterator.seek(self.prefix.bytes());
                self.update_state();
                self.next()
            }
            State::ItemReady => {
                self.state = State::ItemUsed;
                let key = self.iterator.key().unwrap();
                let value = self.iterator.value().unwrap();
                Some(Ok((key, value)))
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.next()
            }
            State::Error(error) => Some(Err(KeyspaceError {
                kind: KeyspaceErrorKind::FailedIterate { source: error.clone() }
            })),
            State::Done => None
        }
    }

    pub(crate) fn seek(&mut self, key: &[u8]) {
        match &self.state {
            State::Done | State::Error(_) => {}
            State::Init => {
                if self.has_valid_prefix(key) {
                    self.iterator.seek(key);
                    self.update_state();
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemReady => {
                let valid_prefix = self.has_valid_prefix(key);
                if valid_prefix {
                    match self.peek().unwrap().unwrap().0.cmp(key) {
                        Ordering::Less => {
                            self.iterator.seek(key);
                            self.update_state();
                        }
                        Ordering::Equal => {}
                        Ordering::Greater => unreachable!("Cannot seek backward.")
                    }
                } else {
                    self.state = State::Done;
                }
            }
            State::ItemUsed => {
                self.advance_and_update_state();
                self.seek(key)
            }
        }
    }

    pub(crate) fn advance_and_update_state(&mut self) {
        assert!(matches!(self.state, State::ItemUsed));
        self.iterator.next();
        self.update_state();
    }

    fn update_state(&mut self) {
        if self.iterator.valid() {
            if self.has_valid_prefix(self.iterator.key().unwrap()) {
                self.state = State::ItemReady;
            } else {
                self.state = State::Done;
            }
        } else if self.iterator.status().is_err() {
            self.state = State::Error(self.iterator.status().err().unwrap().clone());
        } else {
            self.state = State::Done;
        }
    }

    fn has_valid_prefix(&self, key: &[u8]) -> bool {
        return key.len() >= self.prefix.length() && &key[0..self.prefix.length()] == self.prefix.bytes();
    }

    pub fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(mut self) -> Vec<(ByteArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            if item.is_none() {
                break;
            }
            let (key, value) = item.unwrap().unwrap_or_log();
            vec.push((ByteArray::<INLINE_KEY>::copy(key), ByteArray::<INLINE_VALUE>::copy(value)));
        }
        vec
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


