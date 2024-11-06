/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    graph::thing::vertex_attribute::ValueEncodingLength,
    value::{
        decimal_value::Decimal,
        primitive_encoding::{decode_i64, decode_u64, encode_i64, encode_u64},
    },
};
use crate::{
    graph::thing::vertex_attribute::{InlineEncodableAttributeID},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DecimalBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DecimalBytes {
    const INTEGER_LENGTH: usize = i64::BITS as usize / 8;
    const FRACTIONAL_LENGTH: usize = u64::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(fixed: Decimal) -> Self {
        let mut bytes = [0; Self::ENCODED_LENGTH];
        bytes[..Self::INTEGER_LENGTH].copy_from_slice(&encode_i64(fixed.integer_part()));
        bytes[Self::INTEGER_LENGTH..][..Self::FRACTIONAL_LENGTH].copy_from_slice(&encode_u64(fixed.fractional_part()));
        Self { bytes }
    }

    pub fn as_decimal(&self) -> Decimal {
        let integer = decode_i64(self.bytes[..Self::INTEGER_LENGTH].try_into().unwrap());
        let fractional = decode_u64(self.bytes[Self::INTEGER_LENGTH..][..Self::FRACTIONAL_LENGTH].try_into().unwrap());
        Decimal::new(integer, fractional)
    }

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DecimalBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Long.length();

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}
