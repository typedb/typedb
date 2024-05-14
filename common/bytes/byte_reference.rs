/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, ops::Range};

use crate::{byte_array::ByteArray, util::HexBytesFormatter};

/*
TODO: if a ByteReference can be directly sliced (eg. byte_ref[0..10]) this would improve its ergonomics a fair amount
 */

#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ByteReference<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> ByteReference<'bytes> {
    pub const fn new(bytes: &'bytes [u8]) -> ByteReference<'bytes> {
        ByteReference { bytes }
    }

    pub const fn bytes(&self) -> &'bytes [u8] {
        self.bytes
    }

    pub fn into_bytes(self) -> &'bytes [u8] {
        self.bytes
    }

    pub const fn length(&self) -> usize {
        self.bytes().len()
    }

    pub(crate) fn truncate(self, length: usize) -> ByteReference<'bytes> {
        assert!(length <= self.bytes.len());
        ByteReference { bytes: &self.bytes[0..length] }
    }

    pub fn into_range(self, range: Range<usize>) -> ByteReference<'bytes> {
        assert!(range.len() <= self.bytes.len());
        ByteReference { bytes: &self.bytes[range.start..range.end] }
    }
}

impl<'bytes, const INLINE_SIZE: usize> From<&'bytes ByteArray<INLINE_SIZE>> for ByteReference<'bytes> {
    fn from(array: &'bytes ByteArray<INLINE_SIZE>) -> Self {
        array.as_ref()
    }
}

impl fmt::Debug for ByteReference<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ByteReference").field(&HexBytesFormatter(self.bytes())).finish()
    }
}
