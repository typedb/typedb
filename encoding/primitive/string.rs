/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use logger::result::ResultExt;
use storage::snapshot::buffer::BUFFER_INLINE_VALUE;

use crate::error::{EncodingError, EncodingErrorKind};

pub struct StringBytes<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_INLINE_VALUE>,
}

impl<'a> StringBytes<'a> {
    pub fn new(value: ByteArrayOrRef<'a, BUFFER_INLINE_VALUE>) -> Self {
        StringBytes {
            bytes: value
        }
    }

    pub fn build(value: &str) -> Self {
        StringBytes { bytes: ByteArrayOrRef::Array(ByteArray::copy(value.as_bytes())) }
    }

    pub fn build_ref(value: &'a str) -> Self {
        StringBytes { bytes: ByteArrayOrRef::Reference(ByteReference::new(value.as_bytes())) }
    }

    pub fn decode_ref(&self) -> &str {
        std::str::from_utf8(self.bytes.bytes())
            .map_err(|err| {
                EncodingError {
                    kind: EncodingErrorKind::FailedUFT8Decode { bytes: self.bytes.bytes().to_vec().into_boxed_slice(), source: err }
                }
            }).unwrap_or_log()
    }

    pub fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_VALUE> {
        self.bytes
    }

    pub fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_ref()
    }
}

