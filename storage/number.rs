/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::num::NonZeroU64;

use durability::DurabilitySequenceNumber;

// TODO: when we introduce partitioning/remote WAL, we need create a separate type of the sequence number, instead of
//       passing through to the Durability sequence number
pub type SequenceNumber = DurabilitySequenceNumber;

// Option<NonZeroU64> takes 8 bytes, while Option<u64> takes 9 bytes (1 for the option flag + the value)
pub type CausalityNumber = Option<NonZeroU64>;
