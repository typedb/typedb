/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::byte_array::ByteArray;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum Write {
    // Insert KeyValue with a new version. Never conflicts. May represent a brand new key or re-inserting an existing key blindly
    Insert { value: ByteArray<BUFFER_VALUE_INLINE> },
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    Put { value: ByteArray<BUFFER_VALUE_INLINE>, reinsert: Arc<AtomicBool>, known_to_exist: bool },
    // Delete with a new version. Conflicts with Require.
    Delete,
}

impl fmt::Debug for Write {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert { value } => {
                if value.is_empty() {
                    write!(f, "Insert {{}}")
                } else {
                    write!(f, "Insert {{ value: {value:?} }}")
                }
            }
            Self::Put { value, reinsert: _, known_to_exist } => {
                if value.is_empty() {
                    write!(f, "Put {{ known_to_exist: {known_to_exist} }}")
                } else {
                    write!(f, "Put {{ value: {value:?}, known_to_exist: {known_to_exist} }}")
                }
            }
            Self::Delete => write!(f, "Delete"),
        }
    }
}

impl PartialEq for Write {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Insert { value }, Self::Insert { value: other_value }) => value == other_value,
            (
                Self::Put { value, reinsert, known_to_exist },
                Self::Put { value: other_value, reinsert: other_reinsert, known_to_exist: other_known_to_exist },
            ) => {
                (value, reinsert.load(Ordering::Acquire), known_to_exist)
                    == (other_value, other_reinsert.load(Ordering::Acquire), other_known_to_exist)
            }
            (Self::Delete, Self::Delete) => true,
            _ => false,
        }
    }
}

impl Eq for Write {}

impl Write {
    pub(crate) fn is_insert(&self) -> bool {
        matches!(self, Write::Insert { .. })
    }

    pub(crate) fn is_put(&self) -> bool {
        matches!(self, Write::Put { .. })
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Write::Delete)
    }

    pub(crate) fn into_value(self) -> ByteArray<BUFFER_VALUE_INLINE> {
        match self {
            Write::Insert { value } | Write::Put { value, .. } => value,
            Write::Delete => panic!("Buffered delete does not have a value."),
        }
    }

    pub(crate) fn get_value(&self) -> &ByteArray<BUFFER_VALUE_INLINE> {
        match self {
            Write::Insert { value } | Write::Put { value, .. } => value,
            Write::Delete => panic!("Buffered delete does not have a value."),
        }
    }

    pub fn category(&self) -> WriteCategory {
        match self {
            Write::Insert { .. } => WriteCategory::Insert,
            Write::Put { .. } => WriteCategory::Put,
            Write::Delete => WriteCategory::Delete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCategory {
    Insert,
    Put,
    Delete,
}
