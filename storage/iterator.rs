/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, error::Error, fmt, sync::Arc};

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
    keyspace_id: KeyspaceId,
    iterator: KeyspaceRangeIterator,
    open_sequence_number: SequenceNumber,
    storage_counters: StorageCounters,

    // The key of the newest visible version seen so far. Versions of a key are adjacent and ordered newest-first, so
    // the versions following it with the same key are older, and hidden. Overwritten in place to avoid per-key copies.
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    // The next item to return, consumed from `iterator` but not yet returned.
    next_item: NextItem,
}

enum NextItem {
    Unknown,
    // SAFETY: `'static` stands for "owned by `iterator`": like the items of the underlying `DBIterator`, the slices
    // point into the raw iterator's current entry, which stays valid until `iterator` is advanced or sought.
    // Both only happen after `next_item` has been reset to `Unknown`.
    Item { key: &'static [u8], value: &'static [u8] },
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
            keyspace_id: keyspace.id(),
            iterator,
            open_sequence_number,
            storage_counters,
            last_visible_key: None,
            next_item: NextItem::Unknown,
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), MVCCReadError>> {
        self.find_next_state();
        match self.next_item {
            NextItem::Unknown => None,
            NextItem::Item { key, value } => Some(Ok((StorageKeyReference::new_raw(self.keyspace_id, key), value))),
        }
    }

    /// Unless already known, consumes keyspace items up to the next visible inserted version and records it as the next
    /// item. The next item stays unknown if the keyspace iterator is exhausted, or fails: a keyspace error is treated as
    /// the end of the range.
    fn find_next_state(&mut self) {
        if !matches!(self.next_item, NextItem::Unknown) {
            return;
        }
        while let Some(Ok((key, value))) = self.iterator.next() {
            let (key, sequence_number, operation) = MVCCKey::split(key);
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
                    // SAFETY: see `NextItem::Item`
                    let (key, value) =
                        unsafe { std::mem::transmute::<(&[u8], &[u8]), (&'static [u8], &'static [u8])>((key, value)) };
                    self.next_item = NextItem::Item { key, value };
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
        match std::mem::replace(&mut self.next_item, NextItem::Unknown) {
            NextItem::Unknown => None,
            NextItem::Item { key, value } => Some(Ok((StorageKeyReference::new_raw(self.keyspace_id, key), value))),
        }
    }
}

impl Seekable<[u8]> for MVCCRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.find_next_state();
        if let NextItem::Item { key: next_key, .. } = self.next_item {
            if next_key < key {
                self.next_item = NextItem::Unknown;
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
