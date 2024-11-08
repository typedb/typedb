/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{Datelike, NaiveDate};

use crate::{
    graph::thing::vertex_attribute::{InlineEncodableAttributeID, ValueEncodingLength},
    value::{
        boolean_bytes::BooleanBytes,
        primitive_encoding::{decode_i32, encode_i32},
        value_type::ValueType,
    },
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DateBytes {
    bytes: [u8; Self::ENCODED_LENGTH],
}

impl DateBytes {
    const DAYS_LENGTH: usize = i32::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::ENCODED_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date: NaiveDate) -> Self {
        let mut bytes = [0; Self::ENCODED_LENGTH];
        bytes[..Self::DAYS_LENGTH].copy_from_slice(&encode_i32(date.num_days_from_ce()));
        Self { bytes }
    }

    pub fn as_naive_date(&self) -> NaiveDate {
        let days = decode_i32(self.bytes[..Self::DAYS_LENGTH].try_into().unwrap());
        NaiveDate::from_num_days_from_ce_opt(days).unwrap()
    }

    pub fn bytes(&self) -> [u8; Self::ENCODED_LENGTH] {
        self.bytes
    }
}

impl InlineEncodableAttributeID for DateBytes {
    const ENCODED_LENGTH: usize = ValueEncodingLength::Short.length();
    const VALUE_TYPE: ValueType = ValueType::Date;

    fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }

    fn read(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == Self::ENCODED_LENGTH);
        DateBytes::new(bytes.try_into().unwrap())
    }
}
