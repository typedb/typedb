/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::{
    fixed_point_value::FixedPoint,
    primitive_encoding::{decode_i64, decode_u64, encode_i64, encode_u64},
};
use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct FixedPointBytes {
    bytes: [u8; Self::LENGTH],
}

impl FixedPointBytes {
    const LENGTH: usize = AttributeIDLength::Long.length();

    const INTEGER_LENGTH: usize = i64::BITS as usize / 8;
    const FRACTIONAL_LENGTH: usize = u64::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(fixed: FixedPoint) -> Self {
        let mut bytes = [0; Self::LENGTH];
        bytes[..Self::INTEGER_LENGTH].copy_from_slice(&encode_i64(fixed.integer_part()));
        bytes[Self::INTEGER_LENGTH..][..Self::FRACTIONAL_LENGTH].copy_from_slice(&encode_u64(fixed.fractional_part()));
        Self { bytes }
    }

    pub fn as_fixed_point(&self) -> FixedPoint {
        let integer = decode_i64(self.bytes[..Self::INTEGER_LENGTH].try_into().unwrap());
        let fractional = decode_u64(self.bytes[Self::INTEGER_LENGTH..][..Self::FRACTIONAL_LENGTH].try_into().unwrap());
        FixedPoint::new(integer, fractional)
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
