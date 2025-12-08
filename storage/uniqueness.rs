/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::atomic::{AtomicU64, Ordering};

use durability::DurabilitySequenceNumber;
use serde::{Deserialize, Serialize};

// TODO: when we introduce partitioning/remote WAL, we need create a separate type of the sequence number, instead of
//       passing through to the Durability sequence number
pub type SequenceNumber = DurabilitySequenceNumber;

static TRANSACTION_LOCAL_ID_GEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    /// Sequence number at which the transaction was opened.
    /// This lets us efficiently seek into the WAL when checking idempotency.
    open_sequence_number: SequenceNumber,

    /// Per-open-sequence unique value (can be an external monotonic counter).
    local_id: u64,
}

impl TransactionId {
    pub fn new(open_sequence_number: SequenceNumber) -> Self {
        let local_id = TRANSACTION_LOCAL_ID_GEN.fetch_add(1, Ordering::Relaxed);
        Self { open_sequence_number, local_id }
    }

    pub fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    pub fn local_id(&self) -> u64 {
        self.local_id
    }
}
