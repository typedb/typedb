/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use bytes::{byte_reference::ByteReference, Bytes};
use crate::AsBytes;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StructBytes<'a, const INLINE_LENGTH: usize> {
    bytes: Bytes<'a, INLINE_LENGTH>,
}

pub trait StructRepresentation<'a> {
    fn to_bytes<const INLINE_LENGTH: usize>(&self) -> StructBytes<'static, INLINE_LENGTH>;

    fn from_bytes<const INLINE_LENGTH: usize>(struct_bytes: &StructBytes<'a, INLINE_LENGTH>) -> Self;
}

impl<'a, const INLINE_LENGTH: usize> StructBytes<'a, INLINE_LENGTH> {
    pub fn new(value: Bytes<'a, INLINE_LENGTH>) -> Self {
        StructBytes { bytes: value }
    }

    // TODO: There's a slight smell here that I return StructBytes<'static> instead of Self.
    // The values in the StructValue are tied to an <'a> lifetime,
    // because they could refer to the underlying bytes as stored in the datbabase.
    // However, there's no way for me to retrieve the underlying buffer from the struct,
    // (we just store the bytes in the StructValue and deserialize on the fly)
    pub fn build<T: StructRepresentation<'a>>(struct_value: &T) -> StructBytes<'static, INLINE_LENGTH> {
        struct_value.to_bytes()
    }

    pub fn as_struct_representation<T: StructRepresentation<'a>>(&self) -> T {
        T::from_bytes(self)
    }

    pub fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn as_reference(&'a self) -> StructBytes<'a, INLINE_LENGTH> {
        StructBytes { bytes: Bytes::Reference(self.bytes.as_reference()) }
    }

    pub fn into_owned(self) -> StructBytes<'static, INLINE_LENGTH> {
        StructBytes { bytes: self.bytes.into_owned() }
    }
}

impl<'a, const INLINE_LENGTH: usize> AsBytes<'a, INLINE_LENGTH> for StructBytes<'a, INLINE_LENGTH> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, INLINE_LENGTH> {
        self.bytes
    }
}

impl<'a, const INLINE_LENGTH: usize> fmt::Display for StructBytes<'a, INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes(len={})={:?}", self.length(), self.bytes())
    }
}
