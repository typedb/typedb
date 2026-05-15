/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    number: u64,
}

impl SnapshotId {
    pub const UNSET: Self = Self { number: 0 };

    pub const fn from_number(number: u64) -> Self {
        Self { number }
    }

    pub fn number(&self) -> u64 {
        self.number
    }
}
