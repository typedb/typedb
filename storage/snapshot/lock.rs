/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum LockType {
    // Lock an existing key that ensures it cannot be deleted concurrently
    Unmodifiable,
    // Lock a new key to be written exclusively by one snapshot
    Exclusive,
}
