/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

static UNIQUE_ID_GEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    number: u64,
}

impl SnapshotId {
    pub fn new() -> Self {
        Self { number: UNIQUE_ID_GEN.fetch_add(1, Ordering::Relaxed) }
    }

    pub fn number(&self) -> u64 {
        self.number
    }
}
