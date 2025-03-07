/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, error::Error, fmt, sync::Arc};

use bytes::byte_array::ByteArray;
use lending_iterator::{LendingIterator, Peekable, Seekable};
use resource::profile::StorageCounters;

use super::{MVCCKey, MVCCStorage, StorageOperation, MVCC_KEY_INLINE_SIZE};
use crate::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{iterator::KeyspaceRangeIterator, IteratorPool, KeyspaceError, KeyspaceId},
    sequence_number::SequenceNumber,
};

pub(crate) struct MVCCRangeIterator {
    storage_name: Arc<String>,
    keyspace_id: KeyspaceId,
    iterator: Peekable<KeyspaceRangeIterator>,
    open_sequence_number: SequenceNumber,
    storage_counters: StorageCounters,

    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    item: Option<Result<(StorageKeyReference<'static>, &'static [u8]), MVCCReadError>>,
}

impl MVCCRangeIterator {
    //
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1
    // once we find a successful key, to skip all 'older' versions of the key
    //
    pub(crate) fn new<D, const PS: usize>(
        storage: &MVCCStorage<D>,
        iterpool: &IteratorPool,
        range: &KeyRange<StorageKey<'_, PS>>,
        open_sequence_number: SequenceNumber,
        storage_counters: StorageCounters,
    ) -> Self {
        let keyspace = storage.get_keyspace(range.start().get_value().keyspace_id());
        let mapped_range = range.map(|key| key.as_bytes(), |fixed_width| fixed_width);
        let iterator = keyspace.iterate_range(iterpool, &mapped_range, storage_counters.clone());
        MVCCRangeIterator {
            storage_name: storage.name(),
            keyspace_id: keyspace.id(),
            iterator: Peekable::new(iterator),
            open_sequence_number,
            last_visible_key: None,
            item: None,
            storage_counters,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<&Result<(StorageKeyReference<'_>, &[u8]), MVCCReadError>> {
        type Item<'a> = <MVCCRangeIterator as LendingIterator>::Item<'a>;
        if self.item.is_none() {
            self.item = unsafe { std::mem::transmute::<Option<Item<'_>>, Option<Item<'static>>>(self.next()) };
        }
        self.item.as_ref()
    }

    fn find_next_state(&mut self) -> bool {
        while let Some(&Ok((key, _))) = self.iterator.peek() {
            let mvcc_key = MVCCKey::wrap_slice(key);
            let is_visible = mvcc_key.is_visible_to(self.open_sequence_number)
                && !self.last_visible_key.as_ref().is_some_and(|key| key == mvcc_key.key());
            if is_visible {
                self.last_visible_key = Some(ByteArray::copy(mvcc_key.key()));
                match mvcc_key.operation() {
                    StorageOperation::Insert => {
                        return true;
                    }
                    StorageOperation::Delete => {
                        self.storage_counters.increment_advance_mvcc_deleted();
                        self.iterator.next();
                    }
                }
            } else {
                self.storage_counters.increment_advance_mvcc_invisible();
                self.iterator.next();
            }
        }
        return false;
    }
}

impl LendingIterator for MVCCRangeIterator {
    type Item<'a> = Result<(StorageKeyReference<'a>, &'a [u8]), MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if let Some(item) = self.item.take() {
            Some(item)
        } else {
            if self.find_next_state() {
                let (key, value) = match self.iterator.next()? {
                    Ok(kv) => {
                        self.storage_counters.increment_advance_mvcc_visible();
                        kv
                    }
                    Err(error) => {
                        return Some(Err(MVCCReadError::Keyspace {
                            storage_name: self.storage_name.clone(),
                            source: Arc::new(error.clone()),
                        }))
                    }
                };
                Some(Ok((
                    StorageKeyReference::new_raw(
                        self.keyspace_id,
                        MVCCKey::wrap_slice(key).into_key().unwrap_reference(),
                    ),
                    value,
                )))
            } else {
                return None;
            }
        }
    }
}

impl Seekable<[u8]> for MVCCRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        if let Some(Ok((peek, _))) = self.peek() {
            if peek.bytes() < key {
                self.item.take();
                self.iterator.seek(key);
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
    Keyspace { storage_name: Arc<String>, source: Arc<KeyspaceError> },
}

impl fmt::Display for MVCCReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for MVCCReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Keyspace { source, .. } => Some(source),
        }
    }
}
