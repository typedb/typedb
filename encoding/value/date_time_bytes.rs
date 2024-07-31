/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{DateTime, NaiveDateTime};

use crate::graph::thing::vertex_attribute::AttributeIDLength;

use super::primitive_encoding::{decode_i64, decode_u32, encode_i64, encode_u32};

#[derive(Debug, Copy, Clone)]
pub struct DateTimeBytes {
    bytes: [u8; Self::LENGTH],
}

impl DateTimeBytes {
    pub(crate) const LENGTH: usize = AttributeIDLength::Long.length();

    const TIMESTAMP_LENGTH: usize = i64::BITS as usize / 8;
    const NANOS_LENGTH: usize = u32::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date_time: NaiveDateTime) -> Self {
        let date_time = date_time.and_utc();
        let mut bytes = [0; Self::LENGTH];
        bytes[..Self::TIMESTAMP_LENGTH].copy_from_slice(&encode_i64(date_time.timestamp()));
        bytes[Self::TIMESTAMP_LENGTH..][..Self::NANOS_LENGTH]
            .copy_from_slice(&encode_u32(date_time.timestamp_subsec_nanos()));
        Self { bytes }
    }

    pub fn as_naive_date_time(&self) -> NaiveDateTime {
        let secs = decode_i64(self.bytes[..Self::TIMESTAMP_LENGTH].try_into().unwrap());
        let nsecs = decode_u32(self.bytes[Self::TIMESTAMP_LENGTH..][..Self::NANOS_LENGTH].try_into().unwrap());
        DateTime::from_timestamp(secs, nsecs).unwrap().naive_utc()
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
