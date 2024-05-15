/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, marker::PhantomData, sync::Arc};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use iterator::State;

use super::{MVCCKey, MVCCStorage, StorageOperation, MVCC_KEY_INLINE_SIZE};
use crate::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{iterator::KeyspaceRangeIterator, Keyspace, KeyspaceError},
};
use crate::sequence_number::SequenceNumber;

pub(crate) struct MVCCRangeIterator<'storage, const PS: usize> {
    storage_name: &'storage str,
    keyspace: &'storage Keyspace,
    iterator: KeyspaceRangeIterator<'storage, PS>,
    open_sequence_number: SequenceNumber,
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    state: State<Arc<KeyspaceError>>,
    _storage: PhantomData<&'storage MVCCStorage<()>>,
}

impl<'storage, const P: usize> MVCCRangeIterator<'storage, P> {
    //
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1
    // once we find a successful key, to skip all 'older' versions of the key
    //
    pub(crate) fn new<D>(
        storage: &'storage MVCCStorage<D>,
        range: KeyRange<StorageKey<'storage, P>>,
        open_sequence_number: SequenceNumber,
    ) -> Self {
        debug_assert!(!range.start().bytes().is_empty());
        let keyspace = storage.get_keyspace(range.start().keyspace_id());
        let iterator = keyspace.iterate_range(range.map(|key| key.into_bytes(), |fixed_width| fixed_width));
        MVCCRangeIterator {
            storage_name: storage.name(),
            keyspace,
            iterator,
            open_sequence_number,
            last_visible_key: None,
            state: State::Init,
            _storage: PhantomData,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), MVCCReadError>> {
        match &self.state {
            State::Init => {
                self.find_next_state();
                self.peek()
            }
            State::ItemUsed => {
                let _ = self.iterator.next();
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => {
                let (key, value) = match self.iterator.peek()? {
                    Ok(kv) => kv,
                    Err(error) => {
                        return Some(Err(MVCCReadError::Keyspace {
                            storage_name: self.storage_name.to_owned(),
                            source: Arc::new(error),
                        }))
                    }
                };
                let mvcc_key = MVCCKey::wrap_slice(key);
                Some(Ok((
                    StorageKeyReference::new_raw(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )))
            }
            State::Error(error) => {
                Some(Err(MVCCReadError::Keyspace { storage_name: self.storage_name.to_owned(), source: error.clone() }))
            }
            State::Done => None,
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), MVCCReadError>> {
        match &self.state {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::ItemUsed => {
                let _ = self.iterator.next();
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                let (key, value) = match self.iterator.peek()? {
                    Ok(kv) => kv,
                    Err(error) => {
                        return Some(Err(MVCCReadError::Keyspace {
                            storage_name: self.storage_name.to_owned(),
                            source: Arc::new(error),
                        }))
                    }
                };
                let mvcc_key = MVCCKey::wrap_slice(key);
                let item = Some(Ok((
                    StorageKeyReference::new_raw(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )));
                self.state = State::ItemUsed;
                item
            }
            State::Error(error) => {
                Some(Err(MVCCReadError::Keyspace { storage_name: self.storage_name.to_owned(), source: error.clone() }))
            }
            State::Done => None,
        }
    }

    fn find_next_state(&mut self) {
        assert!(matches!(&self.state, State::Init | State::ItemUsed));
        while matches!(&self.state, State::Init | State::ItemUsed) {
            match self.iterator.peek() {
                None => self.state = State::Done,
                Some(Ok((key, _))) => {
                    let mvcc_key = MVCCKey::wrap_slice(key);
                    let is_visible = mvcc_key.is_visible_to(self.open_sequence_number)
                        && !self.last_visible_key.as_ref().is_some_and(|key| key == mvcc_key.key());
                    if is_visible {
                        self.last_visible_key = Some(ByteArray::copy(mvcc_key.key()));
                        match mvcc_key.operation() {
                            StorageOperation::Insert => self.state = State::ItemReady,
                            StorageOperation::Delete => self.advance(),
                        }
                    } else {
                        self.advance()
                    }
                }
                Some(Err(error)) => self.state = State::Error(Arc::new(error)),
            }
        }
    }

    pub(crate) fn seek(&mut self, target: impl AsRef<[u8]>) {
        match self.state {
            State::Init | State::ItemUsed => {
                self.iterator.seek(target.as_ref());
                self.find_next_state()
            }
            State::ItemReady => {
                let (peek, _) = self.peek().unwrap().unwrap();
                if peek.bytes() < target.as_ref() {
                    self.state = State::ItemUsed;
                    self.iterator.seek(target.as_ref());
                    self.find_next_state()
                }
            }
            State::Error(_) | State::Done => {}
        }
    }

    fn advance(&mut self) {
        match self.iterator.next() {
            None => self.state = State::Done,
            Some(Ok(_)) => (),
            Some(Err(error)) => self.state = State::Error(Arc::new(error)),
        }
    }

    pub(crate) fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(
        mut self,
    ) -> Result<Vec<(StorageKeyArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)>, MVCCReadError> {
        let mut vec = Vec::new();
        loop {
            match self.next().transpose()? {
                None => break,
                Some((key, value)) => vec.push((StorageKeyArray::from(key), ByteArray::from(value))),
            }
        }
        Ok(vec)
    }
}

#[derive(Debug, Clone)]
pub enum MVCCReadError {
    Keyspace { storage_name: String, source: Arc<KeyspaceError> },
}

impl fmt::Display for MVCCReadError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for MVCCReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Keyspace { source, .. } => Some(source),
        }
    }
}
