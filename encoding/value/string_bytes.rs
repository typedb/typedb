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
pub struct StringBytes<const INLINE_LENGTH: usize> {
    bytes: ByteArray<INLINE_LENGTH>,
}

impl<const INLINE_LENGTH: usize> StringBytes<INLINE_LENGTH> {
    pub fn new(value: Bytes<'_, INLINE_LENGTH>) -> Self {
        StringBytes { bytes: ByteArray::copy(&value) }
    }

    pub fn build_owned(value: &str) -> Self {
        StringBytes { bytes: ByteArray::copy(value.as_bytes()) }
    }

    pub const fn build_static_ref(value: &str) -> Self {
        StringBytes { bytes: ByteArray::copy_inline(value.as_bytes()) }
    }

    pub fn build_ref(value: &str) -> Self {
        StringBytes { bytes: ByteArray::copy(value.as_bytes()) }
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes)
            .map_err(|err| EncodingError::UFT8Decode { bytes: Box::from(&*self.bytes), source: err })
            .unwrap_or_log()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_reference(&self) -> StringBytes<INLINE_LENGTH> {
        StringBytes { bytes: ByteArray::copy(&self.bytes) }
    }
}

impl<const INLINE_LENGTH: usize> AsBytes<INLINE_LENGTH> for StringBytes<INLINE_LENGTH> {
    fn to_bytes(self) -> Bytes<'static, INLINE_LENGTH> {
        Bytes::Array(self.bytes)
    }
}

impl<const B: usize, const A: usize> PartialEq<StringBytes<B>> for StringBytes<A> {
    fn eq(&self, other: &StringBytes<B>) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<const INLINE_LENGTH: usize> fmt::Debug for StringBytes<INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes(len={}, str='{}')", self.len(), self.as_str())
    }
}

impl<const INLINE_LENGTH: usize> fmt::Display for StringBytes<INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
