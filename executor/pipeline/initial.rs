/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::{LendingIterator, Once};

use crate::{
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct InitialStage<Snapshot> {
    context: ExecutionContext<Snapshot>,
}

impl<Snapshot> InitialStage<Snapshot> {
    pub fn new(context: ExecutionContext<Snapshot>) -> Self {
        Self { context }
    }
}

impl<Snapshot> StageAPI<Snapshot> for InitialStage<Snapshot> {
    type OutputIterator = InitialIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        Ok((InitialIterator::new(), self.context))
    }
}

pub struct InitialIterator {
    single_iterator: Once<Result<MaybeOwnedRow<'static>, PipelineExecutionError>>,
}

impl InitialIterator {
    fn new() -> Self {
        Self { single_iterator: lending_iterator::once(Ok(MaybeOwnedRow::empty())) }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.single_iterator.next()
    }
}

impl StageIterator for InitialIterator {}
