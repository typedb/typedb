/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{fmt, slice};

use compiler::VariablePosition;
use ir::pattern::BranchID;
use tokio::sync::broadcast::error::TryRecvError;

pub mod batch;
pub mod document;
pub mod error;
pub(crate) mod instruction;
pub mod match_executor;
pub mod pipeline;
pub mod read;
pub(crate) mod reduce_executor;
pub mod row;
pub mod write;

// TODO: use a bit-vec, since we have a continuously allocated range of positions
// ---> for now, using a byte vec, which is 8x wasteful and on the heap!
#[derive(Debug)]
pub(crate) struct SelectedPositions {
    selected: Vec<VariablePosition>,
}

impl SelectedPositions {
    fn new(selected: Vec<VariablePosition>) -> Self {
        Self { selected }
    }
}

impl<'a> IntoIterator for &'a SelectedPositions {
    type Item = &'a VariablePosition;

    type IntoIter = slice::Iter<'a, VariablePosition>;

    fn into_iter(self) -> Self::IntoIter {
        self.selected.iter()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum InterruptType {
    TransactionClosed,
    TransactionCommitted,
    TransactionRolledback,
    WriteQueryExecution,
    SchemaQueryExecution,
}

impl fmt::Display for InterruptType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InterruptType::TransactionClosed => write!(f, "transaction close"),
            InterruptType::TransactionCommitted => write!(f, "transaction commit"),
            InterruptType::TransactionRolledback => write!(f, "transaction rollback"),
            InterruptType::WriteQueryExecution => write!(f, "write query"),
            InterruptType::SchemaQueryExecution => write!(f, "schema query"),
        }
    }
}

#[derive(Debug)]
pub struct ExecutionInterrupt {
    signal: Option<tokio::sync::broadcast::Receiver<InterruptType>>,
}

impl ExecutionInterrupt {
    pub fn new(signal: tokio::sync::broadcast::Receiver<InterruptType>) -> Self {
        Self { signal: Some(signal) }
    }

    pub fn new_uninterruptible() -> Self {
        Self { signal: None }
    }

    pub fn check(&mut self) -> Option<InterruptType> {
        // TODO: if this becomes expensive to check frequently (try_recv may acquire locks), we could
        //       optimise it by caching the last time it was checked, and only actually check
        //       the signal once T micros/millis are elapsed... if this is really really cheap we can
        //       check the optimised interrupt in really hot loops as well.
        match &mut self.signal {
            None => None,
            Some(signal) => match signal.try_recv() {
                Ok(type_) => Some(type_),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) | Err(TryRecvError::Lagged(_)) => {
                    unreachable!("Unexpected interrupt signal state. They should never be lagged or closed before cleaning up the receivers.")
                }
            },
        }
    }
}

impl Clone for ExecutionInterrupt {
    // Note: going against tokio's broadcast signal convention, which explicitly isn't `clone()`
    fn clone(&self) -> Self {
        Self { signal: self.signal.as_ref().map(|signal| signal.resubscribe()) }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct Provenance(pub u64);

impl Provenance {
    pub const INITIAL: Provenance = Provenance(0);

    pub(crate) fn set_branch_id(&mut self, id: BranchID) {
        if id.0 < 64 {
            self.0 |= 1 << id.0
        }
    }

    pub fn branch_ids(&self) -> impl Iterator<Item = BranchID> {
        let provenance = self.0;
        (0..64).filter(move |id| 0 != provenance & (1 << id)).map(|id| BranchID(id))
    }
}
