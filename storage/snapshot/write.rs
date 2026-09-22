/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    fmt::Formatter,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use bytes::byte_array::ByteArray;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Visitor},
};

#[derive(Serialize, Deserialize, Clone)]
pub enum Write {
    // Insert KeyValue with a new version. Never conflicts. May represent a brand new key or re-inserting an existing key blindly
    Insert { value: ByteArray<BUFFER_VALUE_INLINE> },
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    Put { value: ByteArray<BUFFER_VALUE_INLINE>, action: Arc<AtomicPutAction>, known_to_exist: KnownToExist },
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

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum KnownToExist {
    // KnownToExist in storage
    // DO NOT MODIFY ENCODING
    Unknown = 0,     // legacy `false`
    Exists = 1,      // legacy `true`
    NonExistent = 2, // new
}

const _: () = {
    assert!(KnownToExist::Unknown as u8 == 0);
    assert!(KnownToExist::Exists as u8 == 1);
    assert!(KnownToExist::NonExistent as u8 == 2);
};

impl fmt::Display for KnownToExist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KnownToExist::Unknown => "unknown",
            KnownToExist::Exists => "exists",
            KnownToExist::NonExistent => "non-existent",
        })
    }
}

// We serialize this as a boolean for forward compatibility with 3.13
// If we do it as u8, that would break forward compatibility but improve recovery performance.
impl Serialize for KnownToExist {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(*self == KnownToExist::Exists)
    }
}

impl<'de> Deserialize<'de> for KnownToExist {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct KnownToExistVisitor;
        impl Visitor<'_> for KnownToExistVisitor {
            type Value = KnownToExist;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`KnownToExist`")
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                const UNKNOWN: u8 = 0;
                const EXISTS: u8 = 1;
                const NON_EXISTENT: u8 = 2;
                match v {
                    UNKNOWN => Ok(KnownToExist::Unknown),
                    EXISTS => Ok(KnownToExist::Exists),
                    NON_EXISTENT => Ok(KnownToExist::NonExistent),
                    other => Err(E::invalid_value(serde::de::Unexpected::Unsigned(other as u64), &self)),
                }
            }
        }

        deserializer.deserialize_u8(KnownToExistVisitor)
    }
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use bytes::byte_array::ByteArray;
    use resource::constants::snapshot::BUFFER_VALUE_INLINE;
    use serde::{Deserialize, Serialize};

    use crate::snapshot::write::KnownToExist;

    #[derive(Serialize, Deserialize, Clone)]
    pub enum V1Write {
        Insert { value: ByteArray<BUFFER_VALUE_INLINE> },
        Put { value: ByteArray<BUFFER_VALUE_INLINE>, reinsert: Arc<AtomicBool>, known_to_exist: bool },
        Delete,
    }

    #[test]
    fn known_to_exist_serializes_the_same_as_bool() {
        let value = ByteArray::<BUFFER_VALUE_INLINE>::copy(&[1, 2, 3]);
        let reinsert = Arc::new(AtomicBool::new(false));
        let known_to_exist_values = vec![
            (false, KnownToExist::Unknown, KnownToExist::Unknown),
            (true, KnownToExist::Exists, KnownToExist::Exists),
            (false, KnownToExist::NonExistent, KnownToExist::Unknown),
        ];
        for (old_known_to_exist, new_known_to_exist, deserialized_known_to_exist) in known_to_exist_values {
            let old_write =
                V1Write::Put { value: value.clone(), reinsert: reinsert.clone(), known_to_exist: old_known_to_exist };
            let new_write = super::Write::Put {
                value: value.clone(),
                reinsert: reinsert.clone(),
                known_to_exist: new_known_to_exist,
            };
            let serialized_old = bincode::serialize(&old_write).unwrap();
            let serialized_new = bincode::serialize(&new_write).unwrap();
            let new_deserialized_as_old: V1Write = bincode::deserialize(&serialized_new).unwrap();
            let old_deserialized_as_new: super::Write = bincode::deserialize(&serialized_old).unwrap();

            assert_eq!(serialized_old, serialized_new);

            match (old_write, new_deserialized_as_old) {
                (
                    V1Write::Put {
                        value: expected_value,
                        reinsert: expected_reinsert,
                        known_to_exist: expected_known_to_exist,
                    },
                    V1Write::Put {
                        value: actual_value,
                        reinsert: actual_reinsert,
                        known_to_exist: actual_known_to_exist,
                    },
                ) => {
                    assert_eq!(expected_value, actual_value);
                    assert_eq!(expected_reinsert.load(Ordering::Relaxed), actual_reinsert.load(Ordering::Relaxed));
                    assert_eq!(expected_known_to_exist, actual_known_to_exist);
                }
                _ => unreachable!(),
            }

            match (new_write, old_deserialized_as_new) {
                (
                    super::Write::Put { value: expected_value, reinsert: expected_reinsert, known_to_exist: _ },
                    super::Write::Put {
                        value: actual_value,
                        reinsert: actual_reinsert,
                        known_to_exist: actual_known_to_exist,
                    },
                ) => {
                    assert_eq!(expected_value, actual_value);
                    assert_eq!(expected_reinsert.load(Ordering::Relaxed), actual_reinsert.load(Ordering::Relaxed));
                    assert_eq!(deserialized_known_to_exist, actual_known_to_exist);
                }
                _ => unreachable!(),
            }
        }
    }
}
