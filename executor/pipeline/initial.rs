/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use compiler::VariablePosition;
use lending_iterator::{LendingIterator, Once};

use crate::{
    pipeline::{
        stage::{StageAPI, StageContext},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct InitialStage<Snapshot> {
    context: StageContext<Snapshot>,
}

impl<Snapshot> InitialStage<Snapshot> {
    pub fn new(context: StageContext<Snapshot>) -> Self {
        Self { context }
    }
}

impl<Snapshot> StageAPI<Snapshot> for InitialStage<Snapshot> {
    type OutputIterator = InitialIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, StageContext<Snapshot>), (PipelineExecutionError, StageContext<Snapshot>)> {
        Ok((InitialIterator::new(), self.context))
    }
}

pub struct InitialIterator {
    single_iterator: Once<Result<MaybeOwnedRow<'static>, PipelineExecutionError>>,
}

impl InitialIterator {
    fn new() -> Self {
        Self { single_iterator: lending_iterator::once(Ok(MaybeOwnedRow::new_owned(Vec::new(), 1))) }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.single_iterator.next()
    }
}

impl StageIterator for InitialIterator {}
