/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, error::Error, fmt, sync::Arc};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use lending_iterator::{LendingIterator, Peekable, Seekable};

use super::{MVCCKey, MVCCStorage, StorageOperation, MVCC_KEY_INLINE_SIZE};
use crate::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::{iterator::KeyspaceRangeIterator, KeyspaceError, KeyspaceId},
    sequence_number::SequenceNumber,
};

pub(crate) struct MVCCRangeIterator {
    storage_name: String,
    keyspace_id: KeyspaceId,
    iterator: Peekable<KeyspaceRangeIterator>,
    open_sequence_number: SequenceNumber,

    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    item: Option<Result<(StorageKeyReference<'static>, ByteReference<'static>), MVCCReadError>>,
}

impl MVCCRangeIterator {
    //
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1
    // once we find a successful key, to skip all 'older' versions of the key
    //
    pub(crate) fn new<D, const PS: usize>(
        storage: &MVCCStorage<D>,
        range: KeyRange<StorageKey<'_, PS>>,
        open_sequence_number: SequenceNumber,
    ) -> Self {
        let keyspace = storage.get_keyspace(range.start().get_value().keyspace_id());
        let iterator = keyspace.iterate_range(range.map(|key| key.into_bytes(), |fixed_width| fixed_width));
        MVCCRangeIterator {
            storage_name: storage.name().to_owned(),
            keyspace_id: keyspace.id(),
            iterator: Peekable::new(iterator),
            open_sequence_number,
            last_visible_key: None,
            item: None,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<&Result<(StorageKeyReference<'_>, ByteReference<'_>), MVCCReadError>> {
        type Item<'a> = <MVCCRangeIterator as LendingIterator>::Item<'a>;
        if self.item.is_none() {
            self.item = unsafe { std::mem::transmute::<Option<Item<'_>>, Option<Item<'static>>>(self.next()) };
        }
        self.item.as_ref()
    }

    fn find_next_state(&mut self) {
        while let Some(&Ok((key, _))) = self.iterator.peek() {
            let mvcc_key = MVCCKey::wrap_slice(key);
            let is_visible = mvcc_key.is_visible_to(self.open_sequence_number)
                && !self.last_visible_key.as_ref().is_some_and(|key| key == mvcc_key.key());
            if is_visible {
                self.last_visible_key = Some(ByteArray::copy(mvcc_key.key()));
                match mvcc_key.operation() {
                    StorageOperation::Insert => break,
                    StorageOperation::Delete => {
                        self.iterator.next();
                    }
                }
            } else {
                self.iterator.next();
            }
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

impl LendingIterator for MVCCRangeIterator {
    type Item<'a> = Result<(StorageKeyReference<'a>, ByteReference<'a>), MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if let Some(item) = self.item.take() {
            Some(item)
        } else {
            self.find_next_state();
            let (key, value) = match self.iterator.next()? {
                Ok(kv) => kv,
                Err(error) => {
                    return Some(Err(MVCCReadError::Keyspace {
                        storage_name: self.storage_name.to_owned(),
                        source: Arc::new(error.clone()),
                    }))
                }
            };
            Some(Ok((
                StorageKeyReference::new_raw(self.keyspace_id, MVCCKey::wrap_slice(key).into_key().unwrap_reference()),
                ByteReference::new(value),
            )))
        }
    }
}

impl Seekable<[u8]> for MVCCRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        if let Some(Ok((peek, _))) = self.peek() {
            if peek.bytes() < key {
                self.iterator.seek(key);
                self.find_next_state();
            }
        }
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        if let Ok((peek, _)) = item {
            peek.bytes().cmp(key)
        } else {
            Ordering::Equal
        }
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
