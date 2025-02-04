/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut, Range},
};

use primitive::prefix::Prefix;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::util::{increment, BytesError, HexBytesFormatter};

#[derive(Clone)]
pub enum ByteArray<const INLINE_BYTES: usize> {
    Inline { bytes: [u8; INLINE_BYTES], len: u8 },
    Boxed(Box<[u8]>),
}

impl<const INLINE_BYTES: usize> ByteArray<INLINE_BYTES> {
    pub const fn empty() -> ByteArray<INLINE_BYTES> {
        ByteArray::Inline { bytes: [0; INLINE_BYTES], len: 0 }
    }

    pub fn zeros(length: usize) -> ByteArray<INLINE_BYTES> {
        if length <= INLINE_BYTES {
            Self::Inline { bytes: [0; INLINE_BYTES], len: length as u8 }
        } else {
            ByteArray::Boxed(vec![0u8; length].into())
        }
    }

    pub fn copy(bytes: &[u8]) -> ByteArray<INLINE_BYTES> {
        if bytes.len() <= INLINE_BYTES {
            let mut inline = [0; INLINE_BYTES];
            inline[..bytes.len()].copy_from_slice(bytes);
            ByteArray::Inline { bytes: inline, len: bytes.len() as u8 }
        } else {
            ByteArray::Boxed(bytes.into())
        }
    }

    pub const fn copy_inline(bytes: &[u8]) -> ByteArray<INLINE_BYTES> {
        assert!(bytes.len() <= INLINE_BYTES);
        let mut inline = [0; INLINE_BYTES];
        let mut i = 0;
        while i < bytes.len() {
            inline[i] = bytes[i];
            i += 1;
        }
        ByteArray::Inline { bytes: inline, len: bytes.len() as u8 }
    }

    pub fn copy_concat<const N: usize>(slices: [&[u8]; N]) -> ByteArray<INLINE_BYTES> {
        let length: usize = slices.iter().map(|slice| slice.len()).sum();
        if length <= INLINE_BYTES {
            let mut data = [0; INLINE_BYTES];
            let mut end = 0;
            for slice in slices {
                data[end..][..slice.len()].copy_from_slice(slice);
                end += slice.len();
            }
            ByteArray::Inline { len: length as u8, bytes: data }
        } else {
            ByteArray::Boxed(slices.concat().into_boxed_slice())
        }
    }

    pub fn inline(bytes: [u8; INLINE_BYTES], len: usize) -> ByteArray<INLINE_BYTES> {
        ByteArray::Inline { bytes, len: len as u8 }
    }

    pub fn boxed(bytes: Box<[u8]>) -> ByteArray<INLINE_BYTES> {
        ByteArray::Boxed(bytes)
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.len());
        match self {
            ByteArray::Inline { len, .. } => *len = length as u8,
            ByteArray::Boxed(boxed) => *boxed = boxed[..length].into(),
        }
    }

    pub fn truncate_range(&mut self, range: Range<usize>) {
        assert!(range.start <= self.len() && range.end <= self.len());
        match self {
            ByteArray::Inline { bytes, len } => {
                *len = range.len() as u8;
                bytes.copy_within(range, 0);
            }
            ByteArray::Boxed(boxed) => *boxed = boxed[range].into(),
        }
    }

    pub fn starts_with(&self, bytes: &[u8]) -> bool {
        self.len() >= bytes.len() && &self[0..bytes.len()] == bytes
    }

    pub fn increment(&mut self) -> Result<(), BytesError> {
        increment(self)
    }
}

impl<const INLINE_BYTES: usize> fmt::Debug for ByteArray<INLINE_BYTES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", HexBytesFormatter::borrowed(self))
    }
}

impl<const BYTES: usize> From<&[u8]> for ByteArray<BYTES> {
    fn from(byte_reference: &[u8]) -> Self {
        ByteArray::copy(byte_reference)
    }
}

impl<const INLINE_BYTES: usize> Hash for ByteArray<INLINE_BYTES> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<const INLINE_SIZE: usize> Serialize for ByteArray<INLINE_SIZE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ByteArray::Inline { bytes, len } => Box::<[u8]>::from(&bytes[..*len as usize]).serialize(serializer),
            ByteArray::Boxed(bytes) => bytes.serialize(serializer),
        }
    }
}

impl<'de, const INLINE_SIZE: usize> Deserialize<'de> for ByteArray<INLINE_SIZE> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::Boxed(Box::deserialize(deserializer)?))
    }
}

impl<const INLINE_SIZE: usize> AsRef<[u8]> for ByteArray<INLINE_SIZE> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl<const INLINE_SIZE: usize> Borrow<[u8]> for ByteArray<INLINE_SIZE> {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl<const INLINE_SIZE: usize> Deref for ByteArray<INLINE_SIZE> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Boxed(bytes) => bytes,
            Self::Inline { bytes, len } => &bytes[..*len as usize],
        }
    }
}

impl<const INLINE_SIZE: usize> DerefMut for ByteArray<INLINE_SIZE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Boxed(bytes) => bytes,
            Self::Inline { bytes, len } => &mut bytes[..*len as usize],
        }
    }
}

impl<const INLINE_BYTES: usize> PartialOrd for ByteArray<INLINE_BYTES> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const INLINE_BYTES: usize> Ord for ByteArray<INLINE_BYTES> {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<const INLINE_BYTES: usize> PartialEq for ByteArray<INLINE_BYTES> {
    fn eq(&self, other: &Self) -> bool {
        (**self).eq(&**other)
    }
}

impl<const INLINE_BYTES: usize> Eq for ByteArray<INLINE_BYTES> {}

impl<const INLINE_BYTES: usize> PartialEq<[u8]> for ByteArray<INLINE_BYTES> {
    fn eq(&self, other: &[u8]) -> bool {
        &**self == other
    }
}

impl<const ARRAY_INLINE_SIZE: usize> Prefix for ByteArray<ARRAY_INLINE_SIZE> {
    fn starts_with(&self, other: &Self) -> bool {
        self.starts_with(other)
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.starts_with(&other)
    }
}
