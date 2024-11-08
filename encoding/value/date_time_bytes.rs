/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{DateTime, NaiveDateTime};

use crate::{
    graph::thing::vertex_attribute::{InlineEncodableAttributeID, ValueEncodingLength},
    value::{
        primitive_encoding::{decode_i64, decode_u32, encode_i64, encode_u32},
        value_type::ValueType,
    },
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DateTimeBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DateTimeBytes {
    const TIMESTAMP_LENGTH: usize = i64::BITS as usize / 8;
    const NANOS_LENGTH: usize = u32::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date_time: NaiveDateTime) -> Self {
        let date_time = date_time.and_utc();
        let mut bytes = [0; Self::ENCODED_LENGTH];
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

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DateTimeBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Long.length();
    const VALUE_TYPE: ValueType = ValueType::DateTime;

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }

    fn read(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == Self::ENCODED_LENGTH);
        DateTimeBytes::new(bytes.try_into().unwrap())
    }
}
