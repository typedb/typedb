/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{DateTime, NaiveDateTime};

use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct DateTimeBytes {
    bytes: [u8; Self::LENGTH],
}

impl DateTimeBytes {
    const LENGTH: usize = AttributeIDLength::Long.length();

    const TIMESTAMP_LENGTH: usize = i64::BITS as usize / 8;
    const NANOS_LENGTH: usize = u32::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date_time: NaiveDateTime) -> Self {
        let date_time = date_time.and_utc();
        let mut bytes = [0; Self::LENGTH];
        bytes[..Self::TIMESTAMP_LENGTH].copy_from_slice(&(date_time.timestamp() ^ i64::MIN).to_be_bytes());
        bytes[Self::TIMESTAMP_LENGTH..][..Self::NANOS_LENGTH]
            .copy_from_slice(&date_time.timestamp_subsec_nanos().to_be_bytes());
        Self { bytes }
    }

    pub fn as_naive_date_time(&self) -> NaiveDateTime {
        let secs = i64::from_be_bytes(self.bytes[..Self::TIMESTAMP_LENGTH].try_into().unwrap()) ^ i64::MIN;
        let nsecs = u32::from_be_bytes(self.bytes[Self::TIMESTAMP_LENGTH..][..Self::NANOS_LENGTH].try_into().unwrap());
        DateTime::from_timestamp(secs, nsecs).unwrap().naive_utc()
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
