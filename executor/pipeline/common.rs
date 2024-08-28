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
    stage: Stage,
    error: Option<PipelineError>,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, Stage> PipelineStageExecutor<Snapshot, Stage>
where
    Snapshot: ReadableSnapshot + 'static,
    Stage: StageAPI<Snapshot>,
{
    pub fn new_impl(stage: Stage) -> Self {
        Self { stage, error: None, phantom: PhantomData }
    }
}

impl<Snapshot, Stage> StageAPI<Snapshot> for PipelineStageExecutor<Snapshot, Stage>
    where
        Snapshot: ReadableSnapshot + 'static,
        Stage: StageAPI<Snapshot>,
{
    type StageIterator = Stage::StageIterator;

    fn into_iterator(self) -> Result<Self::StageIterator, PipelineError> {
        todo!()
    }
    // fn initialise(&mut self) -> Result<(), PipelineError> {
    //     match self.stage.take() {
    //         None | Some(Either::Right(_)) => Err(PipelineError::IllegalState),
    //         Some(Either::Left(stage)) => {
    //             self.stage = Some(Either::Right(stage.into_iterator()?));
    //             Ok(())
    //         }
    //     }
    // }
}

pub struct PipelineStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIteratorAPI<Snapshot>,
{
    iterator: Iterator,
    error: Option<PipelineError>,
    phantom: PhantomData<Snapshot>,
}


impl<Snapshot, Iterator> StageIteratorAPI<Snapshot> for PipelineStageIterator<Snapshot, Iterator>
where
    Snapshot: 'static + ReadableSnapshot,
    Iterator: StageIteratorAPI<Snapshot>,
{
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        self.iterator.try_get_shared_context()
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        self.iterator.finalise_and_into_context()
    }
}

impl<Snapshot, Iterator> LendingIterator for PipelineStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIteratorAPI<Snapshot>,
{
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}
