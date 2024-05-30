/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(super) fn decode_i64(bytes: [u8; 8]) -> i64 {
    i64::from_be_bytes(bytes) ^ i64::MIN
}

pub(super) fn encode_i64(value: i64) -> [u8; 8] {
    (value ^ i64::MIN).to_be_bytes()
}
