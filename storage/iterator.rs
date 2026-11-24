/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering, error::Error, fmt, sync::Arc};

use bytes::byte_array::ByteArray;
use lending_iterator::{LendingIterator, Peekable, Seekable, adaptors::Filter, higher_order::FnHktHelper};
use resource::profile::StorageCounters;

use crate::{
    MVCC_KEY_INLINE_SIZE, MVCCKey, MVCCResolvedKeyValue, MVCCStorage, StorageOperation,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceError, KeyspaceId, iterator::KeyspaceRangeIterator},
    sequence_number::SequenceNumber,
};

/// Iterator over the MVCC-resolved keys in a given range.
pub struct MVCCResolvedRangeIterator {
    storage_name: Arc<str>,
    keyspace_id: KeyspaceId,
    iterator: KeyspaceRangeIterator,
    open_sequence_number: SequenceNumber,
    storage_counters: StorageCounters,

    // The key of the newest visible version seen so far. Versions of a key are adjacent and ordered newest-first, so
    // the versions following it with the same key are older, and hidden. Overwritten in place to avoid per-key copies.
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
}

impl MVCCResolvedRangeIterator {
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
        Self {
            storage_name: storage.name(),
            keyspace_id: keyspace.id(),
            iterator,
            open_sequence_number,
            storage_counters,
            last_visible_key: None,
        }
    }
}

impl LendingIterator for MVCCResolvedRangeIterator {
    type Item<'a> = Result<MVCCResolvedKeyValue<'a>, MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.iterator.next()? {
            Ok((key, value)) => {
                let (key, sequence_number, operation) = MVCCKey::wrap_slice(key).split();
                let storage_key = StorageKeyReference::new_raw(self.keyspace_id, key);
                if sequence_number > self.open_sequence_number {
                    self.storage_counters.increment_advance_mvcc_invisible();
                    Some(Ok(MVCCResolvedKeyValue::future(storage_key, value)))
                } else if self.last_visible_key.as_deref() == Some(key) {
                    self.storage_counters.increment_advance_mvcc_invisible();
                    Some(Ok(MVCCResolvedKeyValue::shadowed(storage_key, value)))
                } else {
                    match &mut self.last_visible_key {
                        Some(last_visible_key) => last_visible_key.set(key),
                        None => self.last_visible_key = Some(ByteArray::copy(key)),
                    }
                    match operation {
                        StorageOperation::Insert => {
                            self.storage_counters.increment_advance_mvcc_visible();
                            Some(Ok(MVCCResolvedKeyValue::visible(storage_key, value)))
                        }
                        StorageOperation::Delete => {
                            self.storage_counters.increment_advance_mvcc_deleted();
                            Some(Ok(MVCCResolvedKeyValue::deleted(storage_key, value)))
                        }
                    }
                }
            }
            Err(err) => Some(Err(MVCCReadError::Keyspace {
                storage_name: self.storage_name.clone(),
                source: Arc::new(err.clone()),
            })),
        }
    }
}

impl Seekable<[u8]> for MVCCResolvedRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        if let Ok(item) = item { item.key.bytes().cmp(key) } else { Ordering::Equal }
    }
}

struct FilterPred(for<'a, 'b> fn(&'a Result<MVCCResolvedKeyValue<'b>, MVCCReadError>) -> bool);

impl Borrow<dyn for<'a, 'b> FnHktHelper<&'a Result<MVCCResolvedKeyValue<'b>, MVCCReadError>, bool>> for FilterPred {
    fn borrow(&self) -> &dyn for<'a, 'b> FnHktHelper<&'a Result<MVCCResolvedKeyValue<'b>, MVCCReadError>, bool> {
        &self.0
    }
}

type Filtered<I> = Filter<I, FilterPred>;

/// Iterator over the MVCC-visible keys in a given range.
pub(crate) struct MVCCVisibleRangeIterator {
    iterator: Peekable<Filtered<MVCCResolvedRangeIterator>>,
}

impl MVCCVisibleRangeIterator {
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1
    // once we find a successful key, to skip all 'older' versions of the key
    pub(crate) fn new<D, const PS: usize>(
        storage: &MVCCStorage<D>,
        iterpool: &IteratorPool,
        range: &KeyRange<StorageKey<'_, PS>>,
        open_sequence_number: SequenceNumber,
        storage_counters: StorageCounters,
    ) -> Self {
        fn pred(item: &Result<MVCCResolvedKeyValue<'_>, MVCCReadError>) -> bool {
            match item {
                Ok(item) => item.is_visible(),
                Err(_) => true,
            }
        }
        MVCCVisibleRangeIterator {
            iterator: Peekable::new(
                MVCCResolvedRangeIterator::new(storage, iterpool, range, open_sequence_number, storage_counters)
                    .filter::<FilterPred, dyn for<'a, 'b> FnHktHelper<&'a Result<MVCCResolvedKeyValue<'b>, MVCCReadError>, bool>>(FilterPred(pred)),
            ),
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, &[u8]), MVCCReadError>> {
        self.find_next_state();
        match self.iterator.get_peeked().as_ref()? {
            &Ok(item @ MVCCResolvedKeyValue { key, value, .. }) => {
                debug_assert!(item.is_visible());
                Some(Ok((key.as_ref(), value)))
            }
            Err(err) => Some(Err(err.clone())),
        }
    }

    fn find_next_state(&mut self) {
        if self.iterator.get_peeked().is_some() {
            return;
        }
        while let Some(next) = self.iterator.peek() {
            let Ok(item) = next else { return };
            if item.is_visible() {
                return;
            } else {
                self.iterator.next();
            }
        }
    }
}

impl LendingIterator for MVCCVisibleRangeIterator {
    type Item<'a> = Result<(StorageKeyReference<'a>, &'a [u8]), MVCCReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.find_next_state();
        match self.iterator.next()? {
            Ok(item @ MVCCResolvedKeyValue { key, value, .. }) => {
                debug_assert!(item.is_visible());
                Some(Ok((key, value)))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

impl Seekable<[u8]> for MVCCVisibleRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        self.find_next_state();
        if let Some(&Ok(MVCCResolvedKeyValue { key: next_key, .. })) = self.iterator.get_peeked() {
            if next_key.bytes() < key {
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
