/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{array, sync::Arc};
use std::collections::HashMap;
use compiler::VariablePosition;

use concept::thing::thing_manager::ThingManager;
use lending_iterator::{AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    pipeline::{PipelineExecutionError, StageAPI, StageIterator},
    row::MaybeOwnedRow,
};

pub struct InitialStage<Snapshot: ReadableSnapshot + 'static> {
    snapshot: Arc<Snapshot>,
}

impl<Snapshot: ReadableSnapshot + 'static> InitialStage<Snapshot> {
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        Self { snapshot }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> StageAPI<Snapshot> for InitialStage<Snapshot> {
    type OutputIterator = InitialIterator;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        HashMap::new()
    }

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        Ok((InitialIterator::new(), self.snapshot))
    }
}

pub struct InitialIterator {
    single_iterator: AsLendingIterator<array::IntoIter<Result<MaybeOwnedRow<'static>, PipelineExecutionError>, 1>>,
}

impl InitialIterator {
    fn new() -> Self {
        Self { single_iterator: AsLendingIterator::new([Ok(MaybeOwnedRow::new_owned(Vec::new(), 1))]) }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.single_iterator.next()
    }
}

impl StageIterator for InitialIterator {}
