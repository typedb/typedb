/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::sequence_number::SequenceNumber;

static UNIQUE_ID_GEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    /// Sequence number at which the snapshot was opened.
    /// This lets us efficiently seek into the WAL when checking idempotency.
    sequence_number: SequenceNumber,

    /// Per-open-sequence unique value (can be an external monotonic counter).
    unique_id: u64,
}

impl SnapshotId {
    pub fn new(sequence_number: SequenceNumber) -> Self {
        let unique_id = UNIQUE_ID_GEN.fetch_add(1, Ordering::Relaxed);
        Self { sequence_number, unique_id }
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }

    pub fn unique_id(&self) -> u64 {
        self.unique_id
    }
}
