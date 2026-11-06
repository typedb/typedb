/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, error::Error, fmt, mem, sync::Arc};

use bytes::byte_array::ByteArray;
use lending_iterator::{LendingIterator, Seekable};
use resource::profile::StorageCounters;

use super::{MVCC_KEY_INLINE_SIZE, MVCCKey, MVCCStorage, StorageOperation};
use crate::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceError, KeyspaceId, iterator::KeyspaceRangeIterator},
    sequence_number::SequenceNumber,
};

pub(crate) struct MVCCRangeIterator {
    storage_name: Arc<str>,
    keyspace_id: KeyspaceId,
    iterator: KeyspaceRangeIterator,
    open_sequence_number: SequenceNumber,
    storage_counters: StorageCounters,

    // The key of the newest visible version seen so far. Versions of a key are adjacent and ordered newest-first, so
    // the versions following it with the same key are older, and hidden. Overwritten in place to avoid per-key copies.
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,

    // The next item to return, consumed from `iterator` but not yet returned.
    // SAFETY: `'static` stands for "owned by `iterator`": like the items of the underlying `DBIterator`, the slices
    // point into the raw iterator's current entry, which stays valid until `iterator` is advanced or sought.
    // Both only happen after `next_item` has been reset to `Unknown`.
    next_item: Option<Result<Item<'static>, MVCCReadError>>,
}

#[derive(Clone, Copy)]
struct Item<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> Item<'a> {
    fn into_key_value(self, keyspace_id: KeyspaceId) -> (StorageKeyReference<'a>, &'a [u8]) {
        (StorageKeyReference::new_raw(keyspace_id, self.key), self.value)
    }
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
            iterator,
            open_sequence_number,
            storage_counters,
            last_visible_key: None,
            next_item: None,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), MVCCReadError>> {
        self.find_next_state();
        match self.next_item.as_ref()? {
            &Ok(item) => Some(Ok(item.into_key_value(self.keyspace_id))),
            Err(err) => Some(Err(err.clone())),
        }
    }

    /// Unless already known, consumes keyspace items up to the next visible inserted version and records it as the next
    /// item. The next item stays unknown if the keyspace iterator is exhausted, or fails: a keyspace error is treated as
    /// the end of the range.
    fn find_next_state(&mut self) {
        if self.next_item.is_some() {
            return;
        }
        while let Some(next) = self.iterator.next() {
            let (key, value) = match next {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    self.next_item = Some(Err(MVCCReadError::Keyspace {
                        storage_name: self.storage_name.clone(),
                        source: Arc::new(err),
                    }));
                    return;
                }
            };
            let key = MVCCKey::wrap_slice(key);
            let (key, sequence_number, operation) = key.split();
            if sequence_number > self.open_sequence_number || self.last_visible_key.as_deref() == Some(key) {
                self.storage_counters.increment_advance_mvcc_invisible();
                continue;
            }
            match &mut self.last_visible_key {
                Some(last_visible_key) => last_visible_key.set(key),
                None => self.last_visible_key = Some(ByteArray::copy(key)),
            }
            match operation {
                StorageOperation::Insert => {
                    self.storage_counters.increment_advance_mvcc_visible();
                    // SAFETY: see `Self::next_item`
                    let item = unsafe { mem::transmute::<Item<'_>, Item<'static>>(Item { key, value }) };
                    self.next_item = Some(Ok(item));
                    return;
                }
                StorageOperation::Delete => self.storage_counters.increment_advance_mvcc_deleted(),
            }
        }
    }
}

impl LendingIterator for MVCCRangeIterator {
    type Item<'a> = Result<(StorageKeyReference<'a>, &'a [u8]), MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.find_next_state();
        Some(self.next_item.take()?.map(|item| item.into_key_value(self.keyspace_id)))
    }
}

impl Seekable<[u8]> for MVCCRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.find_next_state();
        if let Some(Ok(Item { key: next_key, .. })) = self.next_item {
            if next_key < key {
                self.next_item = None;
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
