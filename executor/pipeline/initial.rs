/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct InitialStage<Snapshot> {
    context: ExecutionContext<Snapshot>,
    initial_batch: FixedBatch,
}

impl<Snapshot> InitialStage<Snapshot> {
    pub fn new_empty(context: ExecutionContext<Snapshot>) -> Self {
        Self { context, initial_batch: FixedBatch::SINGLE_EMPTY_ROW }
    }

    pub fn new_with(context: ExecutionContext<Snapshot>, initial_row: MaybeOwnedRow<'_>) -> Self {
        let batch = FixedBatch::from(initial_row);
        Self { context, initial_batch: batch }
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
    iterator: FixedBatchRowIterator,
    index: u32,
}

impl InitialIterator {
    fn new(batch: FixedBatch) -> Self {
        Self { iterator: FixedBatchRowIterator::new(Ok(batch)), index: 0 }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next().map(|result| {
            result.map_err(|err| Box::new(PipelineExecutionError::ReadPatternExecution { typedb_source: err.clone() }))
        })
    }
}

impl StageIterator for InitialIterator {}
