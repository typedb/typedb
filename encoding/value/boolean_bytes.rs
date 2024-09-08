/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct BooleanBytes {
    bytes: [u8; Self::LENGTH],
}

impl BooleanBytes {
    pub const LENGTH: usize = AttributeIDLength::Short.length();

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(bool: bool) -> Self {
        let mut bytes = [0; Self::LENGTH];
        bytes[0] = bool as u8;
        Self { bytes }
    }

    pub fn as_bool(&self) -> bool {
        self.bytes[0] != 0
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
