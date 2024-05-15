/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_array::ByteArray, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

use self::{
    boolean_bytes::BooleanBytes, date_time_bytes::DateTimeBytes, double_bytes::DoubleBytes, long_bytes::LongBytes,
    string_bytes::StringBytes, value_type::ValueType,
};

pub mod boolean_bytes;
pub mod date_time_bytes;
pub mod double_bytes;
pub mod label;
pub mod long_bytes;
pub mod string_bytes;
pub mod value_type;

pub fn encode_value_u64(count: u64) -> ByteArray<BUFFER_VALUE_INLINE> {
    // LE is normally platform-native
    ByteArray::copy(&count.to_le_bytes())
}

pub fn decode_value_u64(bytes: ByteReference<'_>) -> u64 {
    // LE is normally platform-native
    u64::from_le_bytes(bytes.bytes().try_into().unwrap())
}

pub trait ValueEncodable: Clone {
    fn value_type(&self) -> ValueType;

    fn encode_boolean(&self) -> BooleanBytes;

    fn encode_long(&self) -> LongBytes;

    fn encode_double(&self) -> DoubleBytes;

    fn encode_date_time(&self) -> DateTimeBytes;

    fn encode_string<const INLINE_LENGTH: usize>(&self) -> StringBytes<INLINE_LENGTH>;
}
