/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::byte_array::ByteArray;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum LockType {
    // Lock an existing key that ensures it cannot be deleted concurrently
    Unmodifiable,
    // Lock a new key to be written exclusively by one snapshot
    Exclusive,
}

pub fn create_custom_lock_key<'a, I, const SIZE: usize>(key_items: I) -> ByteArray<SIZE>
where
    I: Iterator<Item = &'a [u8]> + Clone,
{
    let total_len = key_items.clone().map(|item| item.len()).sum();
    let mut bytes = ByteArray::zeros(total_len);
    let mut start = 0;
    let mut end;
    for item in key_items {
        end = start + item.len();
        bytes.bytes_mut()[start..end].copy_from_slice(item);
        start = end;
    }
    bytes
}
