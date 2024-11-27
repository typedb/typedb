/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use bytes::{byte_array::ByteArray, Bytes};
use logger::result::ResultExt;

use crate::{error::EncodingError, AsBytes};

/*
 * Both Rust and RocksDB use lexicographical ordering for comparing byte slices.
 * This is the natural representation we want, which guarantees that:
 * "a" < "aa" < "b"
 * So we automatically have the correct sort order for strings, where longer strings come after shorter ones
 * with the same prefix.
 */
#[derive(Clone, Eq, Hash)]
pub struct StringBytes<'a, const INLINE_LENGTH: usize> {
    bytes: Bytes<'a, INLINE_LENGTH>,
}

impl<'a, const INLINE_LENGTH: usize> StringBytes<'a, INLINE_LENGTH> {
    pub fn new(value: Bytes<'a, INLINE_LENGTH>) -> Self {
        StringBytes { bytes: value }
    }

    pub fn build_owned(value: &str) -> Self {
        StringBytes { bytes: Bytes::Array(ByteArray::copy(value.as_bytes())) }
    }

    pub const fn build_ref(value: &'a str) -> Self {
        StringBytes { bytes: Bytes::reference(value.as_bytes()) }
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes)
            .map_err(|err| EncodingError::UFT8Decode { bytes: Box::from(&*self.bytes), source: err })
            .unwrap_or_log()
    }

    pub fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_reference(&'a self) -> StringBytes<'a, INLINE_LENGTH> {
        StringBytes { bytes: Bytes::Reference(&self.bytes) }
    }

    pub fn into_owned(self) -> StringBytes<'static, INLINE_LENGTH> {
        StringBytes { bytes: self.bytes.into_owned() }
    }
}

impl<'a, const INLINE_LENGTH: usize> AsBytes<'a, INLINE_LENGTH> for StringBytes<'a, INLINE_LENGTH> {
    fn into_bytes(self) -> Bytes<'a, INLINE_LENGTH> {
        self.bytes
    }
}

impl<'a, const B: usize, const A: usize> PartialEq<StringBytes<'a, B>> for StringBytes<'a, A> {
    fn eq(&self, other: &StringBytes<'a, B>) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<'a, const INLINE_LENGTH: usize> fmt::Debug for StringBytes<'a, INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes(len={}, str='{}')", self.length(), self.as_str())
    }
}

impl<'a, const INLINE_LENGTH: usize> fmt::Display for StringBytes<'a, INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
