/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use itertools::Either;

use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    pipeline::{StageIteratorAPI, PipelineContext, PipelineError, PipelineStageAPI, StageAPI},
};

pub struct PipelineStageExecutor<Snapshot, Stage>
where
    Snapshot: ReadableSnapshot + 'static,
    Stage: StageAPI<Snapshot>,
{
    stage_or_iterator: Option<Either<Stage, Stage::StageIterator>>,
    error: Option<PipelineError>,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, Stage> PipelineStageExecutor<Snapshot, Stage>
where
    Snapshot: ReadableSnapshot + 'static,
    Stage: StageAPI<Snapshot>,
{
    pub fn new_impl(stage: Stage) -> Self {
        Self { stage_or_iterator: Some(Either::Left(stage)), error: None, phantom: PhantomData }
    }
}

impl<Snapshot, Stage> StageIteratorAPI<Snapshot> for PipelineStageExecutor<Snapshot, Stage>
where
    Stage: StageAPI<Snapshot>,
    Snapshot: 'static + ReadableSnapshot,
{
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match &mut self.stage_or_iterator {
            None | Some(Either::Left(_)) => Err(PipelineError::IllegalState),
            Some(Either::Right(iterator)) => iterator.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self.stage_or_iterator {
            None | Some(Either::Left(_)) => Err(PipelineError::IllegalState),
            Some(Either::Right(iterator)) => iterator.finalise_and_into_context(),
        }
    }
}

impl<Snapshot, Stage> PipelineStageAPI<Snapshot> for PipelineStageExecutor<Snapshot, Stage>
where
    Snapshot: ReadableSnapshot + 'static,
    Stage: StageAPI<Snapshot>,
{
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self.stage_or_iterator.take() {
            None | Some(Either::Right(_)) => Err(PipelineError::IllegalState),
            Some(Either::Left(stage)) => {
                self.stage_or_iterator = Some(Either::Right(stage.into_iterator()?));
                Ok(())
            }
        }
    }
}

impl<Snapshot, Stage> LendingIterator for PipelineStageExecutor<Snapshot, Stage>
where
    Snapshot: ReadableSnapshot + 'static,
    Stage: StageAPI<Snapshot>,
{
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match &mut self.stage_or_iterator {
            None => Some(Err(self.error.clone().unwrap_or(PipelineError::IllegalState))),
            Some(Either::Left(_)) => Some(Err(PipelineError::IllegalState)),
            Some(Either::Right(right)) => right.next(),
        }
    }
}
