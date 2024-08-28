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
    pipeline::{IteratingStageAPI, PipelineContext, PipelineError, PipelineStageAPI, UninitialisedStageAPI},
};

pub struct PipelineStageCommon<Snapshot, PipelineStageType, Initial, Iter>
where
    Snapshot: ReadableSnapshot + 'static,
    PipelineStageType: PipelineStageAPI<Snapshot>,
    Initial: UninitialisedStageAPI<Snapshot, IteratingStage = Iter>,
    Iter: IteratingStageAPI<Snapshot>,
{
    inner: Option<Either<Initial, Iter>>,
    error: Option<PipelineError>,
    phantom: PhantomData<(Snapshot, PipelineStageType)>,
}

impl<Snapshot, PipelineStageType, Initial, Iter> PipelineStageCommon<Snapshot, PipelineStageType, Initial, Iter>
where
    Snapshot: ReadableSnapshot + 'static,
    PipelineStageType: PipelineStageAPI<Snapshot>,
    Initial: UninitialisedStageAPI<Snapshot, IteratingStage = Iter>,
    Iter: IteratingStageAPI<Snapshot>,
{
    pub fn new_impl(uninitialised: Initial) -> Self {
        Self { inner: Some(Either::Left(uninitialised)), error: None, phantom: PhantomData }
    }
}

impl<Snapshot, PipelineStageType, Initial, Iter> IteratingStageAPI<Snapshot>
    for PipelineStageCommon<Snapshot, PipelineStageType, Initial, Iter>
where
    Initial: UninitialisedStageAPI<Snapshot, IteratingStage = Iter>,
    PipelineStageType: PipelineStageAPI<Snapshot>,
    Iter: IteratingStageAPI<Snapshot>,
    Snapshot: 'static + ReadableSnapshot,
{
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match &mut self.inner {
            None | Some(Either::Left(_)) => Err(PipelineError::IllegalState),
            Some(Either::Right(iterating)) => iterating.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self.inner {
            None | Some(Either::Left(_)) => Err(PipelineError::IllegalState),
            Some(Either::Right(iterating)) => iterating.finalise_and_into_context(),
        }
    }
}

impl<Snapshot, PipelineStageType, Initial, Iter> PipelineStageAPI<Snapshot>
    for PipelineStageCommon<Snapshot, PipelineStageType, Initial, Iter>
where
    Snapshot: ReadableSnapshot + 'static,
    PipelineStageType: PipelineStageAPI<Snapshot>,
    Initial: UninitialisedStageAPI<Snapshot, IteratingStage = Iter>,
    Iter: IteratingStageAPI<Snapshot>,
{
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self.inner.take() {
            None | Some(Either::Right(_)) => Err(PipelineError::IllegalState),
            Some(Either::Left(uninitialised)) => {
                self.inner = Some(Either::Right(uninitialised.initialise_and_into_iterator()?));
                Ok(())
            }
        }
    }
}

impl<Snapshot, PipelineStageType, Initial, Iter> LendingIterator
    for PipelineStageCommon<Snapshot, PipelineStageType, Initial, Iter>
where
    Snapshot: ReadableSnapshot + 'static,
    PipelineStageType: PipelineStageAPI<Snapshot>,
    Initial: UninitialisedStageAPI<Snapshot, IteratingStage = Iter>,
    Iter: IteratingStageAPI<Snapshot>,
{
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match &mut self.inner {
            None => Some(Err(self.error.clone().unwrap_or(PipelineError::IllegalState))),
            Some(Either::Left(_)) => Some(Err(PipelineError::IllegalState)),
            Some(Either::Right(right)) => right.next(),
        }
    }
}
