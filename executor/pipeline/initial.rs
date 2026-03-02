/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;

use crate::{batch::{FixedBatch, FixedBatchRowIterator}, pipeline::{
    stage::{ExecutionContext, StageAPI},
    PipelineExecutionError, StageIterator,
}, row::MaybeOwnedRow, ExecutionInterrupt, Provenance};
use crate::batch::{Batch, BatchRowIterator};

pub struct InitialStage<Snapshot> {
    context: ExecutionContext<Snapshot>,
    initial_batch: Batch,
}

impl<Snapshot> InitialStage<Snapshot> {
    pub fn new(context: ExecutionContext<Snapshot>, initial_batch: Batch) -> Self {
        Self { context, initial_batch }
    }
}

impl<Snapshot> StageAPI<Snapshot> for InitialStage<Snapshot> {
    type OutputIterator = InitialIterator;

    fn into_iterator(
        self,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        Ok((InitialIterator::new(self.initial_batch), self.context))
    }
}

pub struct InitialIterator {
    iterator: Box<BatchRowIterator>,
    index: u32,
}

impl InitialIterator {
    fn new(batch: Batch) -> Self {
        Self { iterator: Box::new(batch.into_iterator()), index: 0 }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next().map(|row| {
            Ok(row)
        })
    }
}

impl StageIterator for InitialIterator {}
