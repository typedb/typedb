/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    graph::thing::vertex_attribute::ValueEncodingLength,
    value::{
        duration_value::Duration,
        primitive_encoding::{decode_u32, decode_u64, encode_u32, encode_u64},
    },
};
use crate::{
    graph::thing::vertex_attribute::{InlineEncodableAttributeID},
    value::decimal_bytes::DecimalBytes,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DurationBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DurationBytes {
    const MONTHS_LENGTH: usize = u32::BITS as usize / 8;
    const DAYS_LENGTH: usize = u32::BITS as usize / 8;
    const NANOS_LENGTH: usize = u64::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(duration: Duration) -> Self {
        let Duration { months, days, nanos } = duration;
        let mut bytes = [0; Self::ENCODED_LENGTH];
        bytes[..Self::MONTHS_LENGTH].copy_from_slice(&encode_u32(months));
        bytes[Self::MONTHS_LENGTH..][..Self::DAYS_LENGTH].copy_from_slice(&encode_u32(days));
        bytes[Self::MONTHS_LENGTH + Self::DAYS_LENGTH..][..Self::NANOS_LENGTH].copy_from_slice(&encode_u64(nanos));
        Self { bytes }
    }

    pub fn as_duration(&self) -> Duration {
        let months = decode_u32(self.bytes[..Self::MONTHS_LENGTH].try_into().unwrap());
        let days = decode_u32(self.bytes[Self::MONTHS_LENGTH..][..Self::DAYS_LENGTH].try_into().unwrap());
        let nanos =
            decode_u64(self.bytes[Self::MONTHS_LENGTH + Self::DAYS_LENGTH..][..Self::NANOS_LENGTH].try_into().unwrap());
        Duration { months, days, nanos }
    }

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DurationBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Long.length();

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}
