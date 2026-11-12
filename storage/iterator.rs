/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, error::Error, fmt, sync::Arc};

use bytes::byte_array::ByteArray;
use lending_iterator::{LendingIterator, Peekable, Seekable};
use resource::profile::StorageCounters;

use crate::{
    MVCC_KEY_INLINE_SIZE, MVCCKey, MVCCStorage, StorageOperation,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceError, KeyspaceId, iterator::KeyspaceRangeIterator},
    sequence_number::SequenceNumber,
};

pub(crate) struct MVCCRangeIterator {
    storage_name: Arc<str>,
    keyspace_id: KeyspaceId,
    iterator: Peekable<KeyspaceRangeIterator>,
    open_sequence_number: SequenceNumber,
    storage_counters: StorageCounters,

    // The key of the newest visible version seen so far. Versions of a key are adjacent and ordered newest-first, so
    // the versions following it with the same key are older, and hidden. Overwritten in place to avoid per-key copies.
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
}

impl MVCCRangeIterator {
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1
    // once we find a successful key, to skip all 'older' versions of the key
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
            storage_counters,
            last_visible_key: None,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), MVCCReadError>> {
        self.find_next_state();
        match self.iterator.get_peeked().as_ref()? {
            &Ok((key, value)) => {
                let key = StorageKeyReference::new_raw(self.keyspace_id, MVCCKey::extract_key(key));
                Some(Ok((key, value)))
            }
            Err(err) => Some(Err(MVCCReadError::Keyspace {
                storage_name: self.storage_name.clone(),
                source: Arc::new(err.clone()),
            })),
        }
    }

    fn find_next_state(&mut self) {
        if self.iterator.get_peeked().is_some() {
            return;
        }
        while let Some(next) = self.iterator.peek() {
            let Ok((key, _)) = next else { return };
            let key = MVCCKey::wrap_slice(key);
            let (key, sequence_number, operation) = key.split();
            if sequence_number > self.open_sequence_number || self.last_visible_key.as_deref() == Some(key) {
                self.storage_counters.increment_advance_mvcc_invisible();
                self.iterator.next();
                continue;
            }
            match &mut self.last_visible_key {
                Some(last_visible_key) => last_visible_key.set(key),
                None => self.last_visible_key = Some(ByteArray::copy(key)),
            }
            match operation {
                StorageOperation::Insert => {
                    self.storage_counters.increment_advance_mvcc_visible();
                    return;
                }
                StorageOperation::Delete => {
                    self.storage_counters.increment_advance_mvcc_deleted();
                    self.iterator.next();
                }
            }
        }
    }
}

impl LendingIterator for MVCCRangeIterator {
    type Item<'a> = Result<(StorageKeyReference<'a>, &'a [u8]), MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.find_next_state();
        match self.iterator.next()? {
            Ok((key, value)) => {
                let key = StorageKeyReference::new_raw(self.keyspace_id, MVCCKey::extract_key(key));
                Some(Ok((key, value)))
            }
            Err(err) => Some(Err(MVCCReadError::Keyspace {
                storage_name: self.storage_name.clone(),
                source: Arc::new(err.clone()),
            })),
        }
    }
}

impl Seekable<[u8]> for MVCCRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.find_next_state();
        if let Some(&Ok((next_key, ..))) = self.iterator.get_peeked() {
            if MVCCKey::extract_key(next_key) < key {
                self.iterator.next();
                self.iterator.seek(key);
            }
        }
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        if let Ok((peek, _)) = item { peek.bytes().cmp(key) } else { Ordering::Equal }
    }
}

#[derive(Debug, Clone)]
pub enum MVCCReadError {
    Keyspace { storage_name: Arc<str>, source: Arc<KeyspaceError> },
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
