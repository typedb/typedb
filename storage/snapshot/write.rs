/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
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
    Put { value: ByteArray<BUFFER_VALUE_INLINE>, action: Arc<AtomicPutAction>, known_to_exist: bool },
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
            Self::Put { value, action: _, known_to_exist } => {
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
                Self::Put { value, action, known_to_exist },
                Self::Put { value: other_value, action: other_action, known_to_exist: other_known_to_exist },
            ) => {
                (value, action.load(Ordering::Acquire), known_to_exist)
                    == (other_value, other_action.load(Ordering::Acquire), other_known_to_exist)
            }
            (Self::Delete, Self::Delete) => true,
            _ => false,
        }
    }
}

impl Eq for Write {}

impl Write {
    pub fn is_insert(&self) -> bool {
        matches!(self, Write::Insert { .. })
    }

    pub fn is_put(&self) -> bool {
        matches!(self, Write::Put { .. })
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Write::Delete)
    }

    pub fn intends_insert(&self) -> bool {
        match self {
            Write::Insert { .. } => true,
            Write::Put { action, .. } => action.load(Ordering::Relaxed) == PutAction::Insert,
            Write::Delete => false,
        }
    }

    pub fn is_overwrite(&self) -> bool {
        match self {
            Write::Put { action, .. } => action.load(Ordering::Relaxed) == PutAction::Overwrite,
            Write::Insert { .. } => false,
            Write::Delete => false,
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PutAction {
    Nop = 0,
    Insert = 1,
    Overwrite = 2,
}

impl From<PutAction> for u8 {
    fn from(value: PutAction) -> Self {
        match value {
            PutAction::Nop => 0,
            PutAction::Insert => 1,
            PutAction::Overwrite => 2,
        }
    }
}

impl TryFrom<u8> for PutAction {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Nop),
            1 => Ok(Self::Insert),
            2 => Ok(Self::Overwrite),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct AtomicPutAction {
    inner: AtomicU8,
}

impl AtomicPutAction {
    pub fn new(status: PutAction) -> Self {
        Self { inner: AtomicU8::new(status.into()) }
    }

    pub fn load(&self, order: Ordering) -> PutAction {
        PutAction::try_from(self.inner.load(order)).unwrap()
    }

    pub fn store(&self, value: PutAction, order: Ordering) {
        self.inner.store(value.into(), order);
    }
}

impl Serialize for AtomicPutAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // serializing as AtomicBool to preserve compat with old `reinsert: AtomicBool`
        AtomicBool::new(self.load(Ordering::Relaxed) != PutAction::Nop).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AtomicPutAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // deserializing as AtomicBool to preserve compat with old `reinsert: AtomicBool`
        if AtomicBool::deserialize(deserializer)?.load(Ordering::Relaxed) {
            Ok(Self::new(PutAction::Insert))
        } else {
            Ok(Self::new(PutAction::Nop))
        }
    }
}
