/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

// Safety: this process-local generator produces unique SnapshotIds within a single process
// lifetime. The (open_sequence_number, snapshot_id) pair used for commit idempotency is unique
// across process restarts because open_sequence_number advances through WAL replay.
// Invariants:
//   - Only one process is responsible for opening committable snapshots at a time
//   - Committable snapshots always open at the latest WAL sequence number
//   - Read-only snapshots (possibly at historical positions) do not produce CommitRecords
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
