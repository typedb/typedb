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

use crate::AsBytes;
use crate::error::{EncodingError, EncodingErrorKind};

// TODO: when we write this as a key to storage, we must double check that [AA] follows [A] and isn't after [B] (length independent)

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StringBytes<'a, const INLINE_LENGTH: usize> {
    bytes: ByteArrayOrRef<'a, INLINE_LENGTH>,
}

impl<'a, const INLINE_LENGTH: usize> StringBytes<'a, INLINE_LENGTH> {
    pub fn new(value: ByteArrayOrRef<'a, INLINE_LENGTH>) -> Self {
        StringBytes { bytes: value }
    }

    pub fn build_owned(value: &str) -> Self {
        StringBytes { bytes: ByteArrayOrRef::Array(ByteArray::copy(value.as_bytes())) }
    }

    pub const fn build_ref(value: &'a str) -> Self {
        StringBytes { bytes: ByteArrayOrRef::Reference(ByteReference::new(value.as_bytes())) }
    }

    pub fn clone_as_ref(&'a self) -> StringBytes<'a, INLINE_LENGTH> {
        StringBytes { bytes: ByteArrayOrRef::Reference(self.bytes.as_reference()) }
    }

    pub fn decode(&self) -> &str {
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

    pub fn into_owned(self) -> StringBytes<'static, INLINE_LENGTH> {
        StringBytes { bytes: self.bytes.into_owned() }
    }
}

impl<'a, const INLINE_LENGTH: usize> AsBytes<'a, INLINE_LENGTH> for StringBytes<'a, INLINE_LENGTH> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, INLINE_LENGTH> {
        self.bytes
    }
}

