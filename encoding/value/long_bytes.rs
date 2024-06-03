/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::primitive_encoding::{decode_i64, encode_i64};

#[derive(Debug, Copy, Clone)]
pub struct LongBytes {
    bytes: [u8; Self::LENGTH],
}

impl LongBytes {
    const LENGTH: usize = 8;

    pub fn new(bytes: [u8; LongBytes::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(long: i64) -> Self {
        Self { bytes: encode_i64(long) }
    }

    pub fn as_i64(&self) -> i64 {
        decode_i64(self.bytes)
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};

    use super::LongBytes;

    #[test]
    fn ordering_is_preserved() {
        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");
        for _ in 0..1_000_000 {
            let lhs = rng.gen();
            let rhs = rng.gen();

            let lhs_bytes = LongBytes::build(lhs);
            let rhs_bytes = LongBytes::build(rhs);

            assert_eq!(
                lhs < rhs,
                lhs_bytes.bytes() < rhs_bytes.bytes(),
                "{:e} (0x{:016X}) < {:e} (0x{:016X}) but 0x{:016X} >= 0x{:016X}",
                lhs,
                lhs as u64,
                rhs,
                rhs as u64,
                u64::from_be_bytes(lhs_bytes.bytes()),
                u64::from_be_bytes(rhs_bytes.bytes()),
            )
        }
    }
}
