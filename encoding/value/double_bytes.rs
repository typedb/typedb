/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct DoubleBytes {
    bytes: [u8; Self::LENGTH],
}

impl DoubleBytes {
    const LENGTH: usize = AttributeIDLength::Short.length();

    const ENCODED_NEGATIVE_ZERO: u64 = i64::MAX as u64;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(double: f64) -> Self {
        debug_assert!(double.is_finite(), "Unencodable double value: {double}");
        // IEEE 754 doubles can be interpreted as sign-magnitude integers with the ordering preserved
        // If sign bit is not set (the value is positive), all we need to do is set the sign bit
        // If sign bit is set (the value is negative), the sign bit needs to be unset and the
        // magnitude bits inverted to preserve the ordering
        let encoded_bits = if double.is_sign_positive() { (-double).to_bits() } else { !double.to_bits() };
        Self { bytes: encoded_bits.to_be_bytes() }
    }

    pub fn as_f64(&self) -> f64 {
        let encoded_bits = u64::from_be_bytes(self.bytes);
        if encoded_bits > Self::ENCODED_NEGATIVE_ZERO {
            -f64::from_bits(encoded_bits)
        } else {
            f64::from_bits(!encoded_bits)
        }
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
