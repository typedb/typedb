/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StructBytes<'a, const INLINE_LENGTH: usize> {
    bytes: Bytes<'a, INLINE_LENGTH>,
}

impl<'a, const INLINE_LENGTH: usize> StructBytes<'a, INLINE_LENGTH> {
    pub fn new(value: Bytes<'a, INLINE_LENGTH>) -> Self {
        StructBytes { bytes: value }
    }

    pub fn build(value: &StructValue, struct_definition: &StructDefinition) -> Self {
        // TODO: encode according to value type definition?

        todo!()
    }

    pub fn decode(&self) -> StructValue {
        // TODO: decode

        todo!()
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
        write!(f, "bytes(len={}), map={:?}", self.length(), self.decode())
    }
}