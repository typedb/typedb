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

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
