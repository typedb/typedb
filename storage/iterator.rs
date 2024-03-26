use core::fmt;
use std::{error::Error, marker::PhantomData, sync::Arc};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use durability::SequenceNumber;
use iterator::State;
use primitive::prefix_range::PrefixRange;

use super::{MVCCKey, MVCCStorage, StorageOperation, MVCC_KEY_INLINE_SIZE};
use crate::{
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{
        iterator::KeyspaceRangeIterator,
        keyspace::{Keyspace, KeyspaceError},
    },
};

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
        range: PrefixRange<StorageKey<'storage, P>>,
        open_sequence_number: SequenceNumber,
    ) -> Self {
        debug_assert!(!range.start().bytes().is_empty());
        let keyspace = storage.get_keyspace(range.start().keyspace_id());
        let iterator = keyspace.iterate_range(range.map(|k| k.into_byte_array_or_ref()));
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
            State::ItemReady => {
                let (key, value) = self.iterator.peek().unwrap().unwrap(); // TODO a closer look
                let mvcc_key = MVCCKey::wrap_slice(key);
                Some(Ok((
                    StorageKeyReference::new_raw(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )))
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(MVCCReadError::Keyspace {
                storage_name: self.storage_name.to_owned(),
                keyspace_name: self.keyspace.name().to_owned(),
                source: error.clone(),
            })),
            State::Done => None,
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), MVCCReadError>> {
        match &self.state {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                let (key, value) = self.iterator.peek().unwrap().unwrap();
                let mvcc_key = MVCCKey::wrap_slice(key);
                let item = Some(Ok((
                    StorageKeyReference::new_raw(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )));
                self.state = State::ItemUsed;
                item
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.next()
            }
            State::Error(error) => Some(Err(MVCCReadError::Keyspace {
                storage_name: self.storage_name.to_owned(),
                keyspace_name: self.keyspace.name().to_owned(),
                source: error.clone(),
            })),
            State::Done => None,
        }
    }

    fn find_next_state(&mut self) {
        assert!(matches!(&self.state, &State::Init) || matches!(&self.state, &State::ItemUsed));
        while matches!(&self.state, &State::Init) || matches!(&self.state, &State::ItemUsed) {
            let peek = self.iterator.peek();
            match peek {
                None => self.state = State::Done,
                Some(Ok((key, _))) => {
                    let mvcc_key = MVCCKey::wrap_slice(key);
                    let is_visible =
                        Self::is_visible_key(&self.open_sequence_number, &self.last_visible_key, &mvcc_key);
                    if is_visible {
                        self.last_visible_key = Some(ByteArray::copy(mvcc_key.key()));
                        match mvcc_key.operation() {
                            StorageOperation::Insert => self.state = State::ItemReady,
                            StorageOperation::Delete => {}
                        }
                    } else {
                        self.advance()
                    }
                }
                Some(Err(error)) => self.state = State::Error(Arc::new(error)),
            }
        }
    }

    fn is_visible_key(
        open_sequence_number: &SequenceNumber,
        last_visible_key: &Option<ByteArray<128>>,
        mvcc_key: &MVCCKey<'_>,
    ) -> bool {
        (last_visible_key.is_none() || last_visible_key.as_ref().unwrap().bytes() != mvcc_key.key())
            && mvcc_key.is_visible_to(open_sequence_number)
    }

    fn advance_and_find_next_state(&mut self) {
        assert!(matches!(self.state, State::ItemUsed));
        self.advance();
        self.find_next_state();
    }

    fn advance(&mut self) {
        let _ = self.iterator.next();
    }

    pub(crate) fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(
        mut self,
    ) -> Result<Vec<(StorageKeyArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)>, MVCCReadError> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            match item {
                None => break,
                Some(Err(err)) => return Err(err),
                Some(Ok((key, value))) => vec.push((StorageKeyArray::from(key), ByteArray::from(value))),
            }
        }
        Ok(vec)
    }
}

#[derive(Debug)]
pub enum MVCCReadError {
    Keyspace { storage_name: String, keyspace_name: String, source: Arc<KeyspaceError> },
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
