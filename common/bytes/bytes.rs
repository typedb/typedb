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
    ops::{Deref, Range},
};

use lending_iterator::higher_order::Hkt;
use primitive::prefix::Prefix;

use crate::{byte_array::ByteArray, byte_reference::ByteReference};

pub mod byte_array;
pub mod byte_reference;
pub mod util;

#[derive(Debug)]
pub enum Bytes<'bytes, const ARRAY_INLINE_SIZE: usize> {
    Array(ByteArray<ARRAY_INLINE_SIZE>),
    Reference(ByteReference<'bytes>),
}

impl<'bytes, const INLINE_SIZE: usize> Clone for Bytes<'bytes, INLINE_SIZE> {
    fn clone(&self) -> Bytes<'static, INLINE_SIZE> {
        match self {
            Bytes::Array(array) => Bytes::Array(array.clone()),
            Bytes::Reference(reference) => Bytes::Array(ByteArray::from(*reference)),
        }
    }
}

impl<const ARRAY_INLINE_SIZE: usize> Bytes<'static, ARRAY_INLINE_SIZE> {
    pub fn copy(bytes: &[u8]) -> Self {
        Self::Array(ByteArray::copy(bytes))
    }

    pub fn inline(bytes: [u8; ARRAY_INLINE_SIZE], length: usize) -> Self {
        Self::Array(ByteArray::inline(bytes, length))
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Bytes<'bytes, ARRAY_INLINE_SIZE> {
    pub const fn reference(bytes: &'bytes [u8]) -> Self {
        Self::Reference(ByteReference::new(bytes))
    }

    pub fn length(&self) -> usize {
        match self {
            Bytes::Array(array) => array.len(),
            Bytes::Reference(reference) => reference.length(),
        }
    }

    pub fn truncate(self, length: usize) -> Bytes<'bytes, ARRAY_INLINE_SIZE> {
        assert!(length <= self.length());
        match self {
            Bytes::Array(mut array) => {
                array.truncate(length);
                Bytes::Array(array)
            }
            Bytes::Reference(reference) => Bytes::Reference(reference.truncate(length)),
        }
    }

    pub fn into_range(self, range: Range<usize>) -> Bytes<'bytes, ARRAY_INLINE_SIZE> {
        assert!(range.start <= self.length() && range.end <= self.length());
        match self {
            Bytes::Array(mut array) => {
                array.truncate_range(range);
                Bytes::Array(array)
            }
            Bytes::Reference(reference) => Bytes::Reference(reference.into_range(range)),
        }
    }

    pub fn to_owned(&self) -> Bytes<'static, ARRAY_INLINE_SIZE> {
        Bytes::Array(self.to_array())
    }

    pub fn into_owned(self) -> Bytes<'static, ARRAY_INLINE_SIZE> {
        Bytes::Array(self.into_array())
    }

    pub fn as_reference(&'bytes self) -> ByteReference<'bytes> {
        match self {
            Bytes::Array(array) => array.as_ref(),
            Bytes::Reference(reference) => *reference,
        }
    }

    pub fn unwrap_reference(self) -> ByteReference<'bytes> {
        if let Bytes::Reference(reference) = self {
            reference
        } else {
            panic!("{} cannot be unwrapped as a reference", self)
        }
    }

    pub fn into_array(self) -> ByteArray<ARRAY_INLINE_SIZE> {
        match self {
            Bytes::Array(array) => array,
            Bytes::Reference(byte_reference) => ByteArray::from(byte_reference),
        }
    }

    pub fn to_array(&self) -> ByteArray<ARRAY_INLINE_SIZE> {
        match self {
            Bytes::Array(array) => array.clone(),
            Bytes::Reference(byte_reference) => ByteArray::from(*byte_reference),
        }
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> fmt::Display for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Deref for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Bytes::Array(array) => array,
            Bytes::Reference(reference) => reference.bytes(),
        }
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> PartialEq for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        (**self).eq(&**other)
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Eq for Bytes<'bytes, ARRAY_INLINE_SIZE> {}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> PartialOrd for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Ord for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Hash for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Borrow<[u8]> for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Prefix for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn starts_with(&self, other: &Self) -> bool {
        (**self).starts_with(other)
    }

    fn into_starts_with(self, other: Self) -> bool {
        (*self).starts_with(&other)
    }
}

impl<const ARRAY_INLINE_SIZE: usize> Hkt for Bytes<'static, ARRAY_INLINE_SIZE> {
    type HktSelf<'a> = Bytes<'a, ARRAY_INLINE_SIZE>;
}
