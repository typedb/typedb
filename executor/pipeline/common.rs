/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
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
    fn try_finalise_and_get_owned_context(mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        if self.next().is_some() { // This changes the state, but we're going to error anyway
            Err(PipelineError::FinalisedUnconsumedStage)
        } else {
            self.context.try_into_owned().map_err(|_| {
                PipelineError::CouldNotGetOwnedContext
            })
        }
    }

    // fn try_get_shared_reference(mut self) -> Result<PipelineContext<Snapshot>, ()> {
    //     match self.context {
    //         PipelineContext::Shared(arc_snapshot, arc_thing_manager) => {
    //             Ok(PipelineContext::Shared(arc_snapshot.clone(), arc_thing_manager.clone()))
    //         }
    //         PipelineContext::Owned(snapshot, thing_manager) => {
    //             let arc_snapshot = Arc::new(snapshot);
    //             let arc_thing_manager = Arc::new(thing_manager);
    //             self.context = PipelineContext::Shared(arc_snapshot.clone(), arc_thing_manager.clone());
    //             Ok(PipelineContext::Shared(arc_snapshot.clone(), arc_thing_manager.clone()))
    //         }
    //     }
    // }
    //
    // fn try_finalise_and_drop_shared_context(mut self) -> Result<(), PipelineError> {
    //     if self.next().is_some() { // This changes the state, but we're going to error anyway
    //         Err(PipelineError::FinalisedUnconsumedStage)
    //     }
    //     Ok(())
    //     // dropping self at the end should be enough
    // }
}
