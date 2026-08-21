/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use bytes::{Bytes, byte_array::ByteArray};

use crate::AsBytes;

/// The storage encoding of a vector value: each f32 element's raw bit pattern, little-endian,
/// concatenated. There is no header: the element count and precision are declared on the value
/// type ([`crate::value::value_type::VectorTypeParameters`]), not stored per value.
/// Equality (and therefore attribute dedup) is bitwise over these bytes, matching the
/// bit-pattern `Hash` impl of `Value::Vector`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct VectorBytes<'a, const INLINE_LENGTH: usize> {
    bytes: Bytes<'a, INLINE_LENGTH>,
}

impl<'a, const INLINE_LENGTH: usize> VectorBytes<'a, INLINE_LENGTH> {
    pub const ELEMENT_LENGTH: usize = size_of::<f32>();

    pub fn new(bytes: Bytes<'a, INLINE_LENGTH>) -> Self {
        debug_assert_eq!(bytes.length() % Self::ELEMENT_LENGTH, 0);
        VectorBytes { bytes }
    }

    pub fn build(vector: &[f32]) -> VectorBytes<'static, INLINE_LENGTH> {
        let mut buf = Vec::with_capacity(vector.len() * Self::ELEMENT_LENGTH);
        for element in vector {
            buf.extend_from_slice(&element.to_bits().to_le_bytes());
        }
        VectorBytes::new(Bytes::Array(ByteArray::boxed(buf.into_boxed_slice())))
    }

    pub fn as_vector(&self) -> Vec<f32> {
        self.bytes
            .chunks_exact(Self::ELEMENT_LENGTH)
            .map(|chunk| f32::from_bits(u32::from_le_bytes(chunk.try_into().unwrap())))
            .collect()
    }

    pub fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn bytes(&'a self) -> &'a [u8] {
        &self.bytes
    }

    pub fn as_reference(&'a self) -> VectorBytes<'a, INLINE_LENGTH> {
        VectorBytes { bytes: Bytes::Reference(&self.bytes) }
    }

    pub fn into_owned(self) -> VectorBytes<'static, INLINE_LENGTH> {
        VectorBytes { bytes: self.bytes.into_owned() }
    }
}

impl<const INLINE_LENGTH: usize> AsBytes<INLINE_LENGTH> for VectorBytes<'_, INLINE_LENGTH> {
    fn to_bytes(self) -> Bytes<'static, INLINE_LENGTH> {
        Bytes::copy(&self.bytes)
    }
}

impl<const INLINE_LENGTH: usize> fmt::Display for VectorBytes<'_, INLINE_LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes(len={})={:?}", self.length(), self.bytes())
    }
}

#[cfg(test)]
mod test {
    use super::VectorBytes;

    #[test]
    fn encoding_decoding() {
        let vector = [1.0f32, -0.0, 0.5, f32::MIN_POSITIVE, 384.25];
        let vector_bytes: VectorBytes<'static, 64> = VectorBytes::build(&vector);
        assert_eq!(vector_bytes.length(), vector.len() * VectorBytes::<64>::ELEMENT_LENGTH);
        let decoded = vector_bytes.as_vector();
        assert_eq!(
            decoded.iter().map(|f| f.to_bits()).collect::<Vec<_>>(),
            vector.iter().map(|f| f.to_bits()).collect::<Vec<_>>()
        );
    }
}
