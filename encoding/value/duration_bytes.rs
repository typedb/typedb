/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::duration_value::Duration;
use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct DurationBytes {
    bytes: [u8; Self::LENGTH],
}

impl DurationBytes {
    const LENGTH: usize = AttributeIDLength::Long.length();

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(duration: Duration) -> Self {
        let Duration { months, days, seconds, nanos } = duration;
        let mut bytes = [0; Self::LENGTH];
        for (i, field) in [months, days, seconds, nanos].into_iter().enumerate() {
            bytes[i * 4..][..4].copy_from_slice(&field.to_be_bytes());
        }
        Self { bytes }
    }

    pub fn as_duration(&self) -> Duration {
        let months = u32::from_be_bytes(self.bytes[..4].try_into().unwrap());
        let days = u32::from_be_bytes(self.bytes[4..][..4].try_into().unwrap());
        let seconds = u32::from_be_bytes(self.bytes[8..][..4].try_into().unwrap());
        let nanos = u32::from_be_bytes(self.bytes[12..][..4].try_into().unwrap());
        Duration { months, days, seconds, nanos }
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
