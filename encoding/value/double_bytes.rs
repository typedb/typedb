/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    graph::thing::vertex_attribute::{InlineEncodableAttributeID},
    graph::thing::vertex_attribute::ValueEncodingLength,
    value::primitive_encoding::{decode_u64, encode_u64},
};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct DoubleBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DoubleBytes {
    const ENCODED_NEGATIVE_ZERO: u64 = i64::MAX as u64;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(double: f64) -> Self {
        debug_assert!(double.is_finite(), "Unencodable double value: {double}");
        // IEEE 754 doubles can be interpreted as sign-magnitude integers with the ordering preserved
        // If sign bit is not set (the value is positive), all we need to do is set the sign bit
        // If sign bit is set (the value is negative), the sign bit needs to be unset and the
        // magnitude bits inverted to preserve the ordering
        let encoded_bits = if double.is_sign_positive() { (-double).to_bits() } else { !double.to_bits() };
        Self { bytes: encode_u64(encoded_bits) }
    }

    pub fn as_f64(&self) -> f64 {
        let encoded_bits = decode_u64(self.bytes);
        if encoded_bits > Self::ENCODED_NEGATIVE_ZERO {
            -f64::from_bits(encoded_bits)
        } else {
            f64::from_bits(!encoded_bits)
        }
    }

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DoubleBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Short.length();

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};

    use super::DoubleBytes;

    fn random_finite_f64(rng: &mut impl Rng) -> f64 {
        loop {
            let float = f64::from_bits(rng.next_u64());
            if float.is_finite() {
                break float;
            }
        }
    }

    #[test]
    fn ordering_is_preserved() {
        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");
        for _ in 0..1_000_000 {
            let lhs = random_finite_f64(&mut rng);
            let rhs = random_finite_f64(&mut rng);

            let lhs_bytes = DoubleBytes::build(lhs);
            let rhs_bytes = DoubleBytes::build(rhs);

            assert_eq!(
                lhs < rhs,
                lhs_bytes.bytes() < rhs_bytes.bytes(),
                "{:e} (0x{:016X}) < {:e} (0x{:016X}) but 0x{:016X} >= 0x{:016X}",
                lhs,
                lhs.to_bits(),
                rhs,
                rhs.to_bits(),
                u64::from_be_bytes(lhs_bytes.bytes()),
                u64::from_be_bytes(rhs_bytes.bytes()),
            )
        }
    }
}
