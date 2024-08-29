/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use compiler::match_::planner::pattern_plan::MatchProgram;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::MaybeOwnedRow,
    pattern_executor::PatternExecutor,
    pipeline::{PipelineError, StageAPI, StageIterator},
};

pub struct MatchStageExecutor<Snapshot: ReadableSnapshot + 'static, PreviousStage: StageAPI<Snapshot>> {
    program: MatchProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot, PreviousStage: StageAPI<Snapshot>> StageAPI<Snapshot>
    for MatchStageExecutor<Snapshot, PreviousStage>
{
    type OutputIterator = MatchStageIterator<PreviousStage::OutputIterator>;

    fn into_iterator(mut self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        let Self { previous: previous_stage, program, .. } = self;
        let (previous_iterator, snapshot, thing_manager) = previous_stage.into_iterator()?;

        let iterator = previous_iterator.try_flat_map(|row| {
            let snapshot = snapshot.clone();
            let thing_manager = thing_manager.clone();
            // TODO: use `row` as input into the executor
            PatternExecutor::new(&program, snapshot.as_ref(), thing_manager.as_ref())
                .map(|executor| {
                    executor
                        .into_iterator(snapshot.clone(), thing_manager.clone())
                        .map(|result| result.map_err(|err| PipelineError::ConceptRead(err.clone())))
                })
                .map_err(|err| PipelineError::InitialisingMatchIterator(err.clone()))
        });
        Ok((MatchStageIterator::new(iterator), snapshot, thing_manager))
    }
}

pub struct MatchStageIterator<Iterator> {
    iterator: Iterator,
}

impl<Iterator> MatchStageIterator<Iterator>
where
    Iterator: for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>>,
{
    fn new(iterator: Iterator) -> Self {
        Self { iterator }
    }
}

impl<Iterator> LendingIterator for MatchStageIterator<Iterator>
where
    Iterator: for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>>,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

impl<Iterator> StageIterator for MatchStageIterator<Iterator> where
    Iterator: for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>>
{
}
