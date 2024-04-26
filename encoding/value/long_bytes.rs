/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Copy, Clone)]
pub struct LongBytes {
    bytes: [u8; LongBytes::LENGTH],
}

impl LongBytes {
    const LENGTH: usize = 8;

    pub fn new(bytes: [u8; LongBytes::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(long: i64) -> Self {
        Self { bytes: (long ^ i64::MIN).to_be_bytes() }
    }

    pub fn as_i64(&self) -> i64 {
        i64::from_be_bytes(self.bytes) ^ i64::MIN
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
