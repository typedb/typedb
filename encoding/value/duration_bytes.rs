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

    const MONTHS_LENGTH: usize = u32::BITS as usize / 8;
    const DAYS_LENGTH: usize = u32::BITS as usize / 8;
    const NANOS_LENGTH: usize = u64::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(duration: Duration) -> Self {
        let Duration { months, days, nanos } = duration;
        let mut bytes = [0; Self::LENGTH];
        bytes[..Self::MONTHS_LENGTH].copy_from_slice(&months.to_be_bytes());
        bytes[Self::MONTHS_LENGTH..][..Self::DAYS_LENGTH].copy_from_slice(&days.to_be_bytes());
        bytes[Self::MONTHS_LENGTH + Self::DAYS_LENGTH..][..Self::NANOS_LENGTH].copy_from_slice(&nanos.to_be_bytes());
        Self { bytes }
    }

    pub fn as_duration(&self) -> Duration {
        let months = u32::from_be_bytes(self.bytes[..Self::MONTHS_LENGTH].try_into().unwrap());
        let days = u32::from_be_bytes(self.bytes[Self::MONTHS_LENGTH..][..Self::DAYS_LENGTH].try_into().unwrap());
        let nanos = u64::from_be_bytes(
            self.bytes[Self::MONTHS_LENGTH + Self::DAYS_LENGTH..][..Self::NANOS_LENGTH].try_into().unwrap(),
        );
        Duration { months, days, nanos }
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
