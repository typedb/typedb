/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
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