/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

pub mod label;
pub mod long;
pub mod string;
pub mod value_type;

pub fn encode_value_u64(count: u64) -> ByteArray<BUFFER_VALUE_INLINE> {
    // LE is normally platform-native
    ByteArray::copy(&count.to_le_bytes())
}

pub fn decode_value_u64(bytes: ByteReference<'_>) -> u64 {
    // LE is normally platform-native
    u64::from_le_bytes(bytes.bytes().try_into().unwrap())
}
