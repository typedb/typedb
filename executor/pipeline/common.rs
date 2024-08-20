/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, BatchRowIterator, ImmutableRow},
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI},
};

pub struct InitialStage<Snapshot: ReadableSnapshot + 'static> {
    context: PipelineContext<Snapshot>,
    only_entry: BatchRowIterator,
}

impl<Snapshot: ReadableSnapshot + 'static> InitialStage<Snapshot> {
    pub fn new(context: PipelineContext<Snapshot>) -> Self {
        Self { context, only_entry: BatchRowIterator::new(Ok(Batch::SINGLE_EMPTY_ROW)) }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for InitialStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;
    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.only_entry.next().map(|result| result.map_err(|source| PipelineError::ConceptRead(source.clone())))
    }
}

impl<Snapshot: ReadableSnapshot + 'static> PipelineStageAPI<Snapshot> for InitialStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        self.context
    }
}
