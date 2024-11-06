/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::DateTime;

use crate::graph::thing::vertex_attribute::{InlineEncodableAttributeID};
use crate::{
    graph::thing::vertex_attribute::ValueEncodingLength,
    value::{date_time_bytes::DateTimeBytes, timezone::TimeZone},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DateTimeTZBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DateTimeTZBytes {
    const DATE_TIME_LENGTH: usize = (i64::BITS + u32::BITS) as usize / 8;
    const TZ_LENGTH: usize = u32::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date_time: DateTime<TimeZone>) -> Self {
        let mut bytes = DateTimeBytes::build(date_time.naive_utc()).bytes();
        bytes[Self::DATE_TIME_LENGTH..][..Self::TZ_LENGTH].copy_from_slice(&date_time.timezone().encode());
        Self { bytes }
    }

    pub fn as_date_time(&self) -> DateTime<TimeZone> {
        let date_time = DateTimeBytes::new(self.bytes).as_naive_date_time();
        let tz = TimeZone::decode(self.bytes[Self::DATE_TIME_LENGTH..][..Self::TZ_LENGTH].try_into().unwrap());
        date_time.and_utc().with_timezone(&tz)
    }

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DateTimeTZBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Long.length();

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}
