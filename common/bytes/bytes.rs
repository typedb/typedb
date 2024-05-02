/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    hash::{Hash, Hasher},
};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::Range;
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

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Bytes<'bytes, ARRAY_INLINE_SIZE> {
    pub fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            Bytes::Array(array) => array.bytes(),
            Bytes::Reference(reference) => reference.bytes(),
        }
    }

    pub fn length(&self) -> usize {
        match self {
            Bytes::Array(array) => array.length(),
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
        write!(f, "{}", dbg!(self))
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> PartialEq for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes().eq(other.bytes())
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
        self.bytes().cmp(other.bytes())
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Hash for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes().hash(state)
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Borrow<[u8]> for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn borrow(&self) -> &[u8] {
        self.bytes()
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Prefix for Bytes<'bytes, ARRAY_INLINE_SIZE> {
    fn starts_with(&self, other: &Self) -> bool {
        self.bytes().starts_with(other.bytes())
    }
}
