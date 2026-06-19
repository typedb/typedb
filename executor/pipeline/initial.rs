/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;

use crate::{
    ExecutionInterrupt, Provenance,
    batch::{Batch, BatchRowIterator, FixedBatch, FixedBatchRowIterator},
    pipeline::{
        PipelineExecutionError, StageIterator,
        stage::{ExecutionContext, StageAPI},
    },
    row::MaybeOwnedRow,
};

pub struct InitialStage {
    initial_batch: Batch,
}

impl InitialStage {
    pub fn new(initial_batch: Batch) -> Self {
        Self { initial_batch }
    }

    pub(crate) fn into_iterator(self) -> InitialIterator {
        InitialIterator::new(self.initial_batch)
    }
}

pub struct InitialIterator {
    iterator: Box<BatchRowIterator>,
}

impl InitialIterator {
    pub(crate) fn new(batch: Batch) -> Self {
        Self { iterator: Box::new(batch.into_iterator()) }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next().map(|row| Ok(row))
    }
}

impl StageIterator for InitialIterator {}
