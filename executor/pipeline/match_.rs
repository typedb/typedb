/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use std::sync::Arc;

use compiler::match_::planner::pattern_plan::MatchProgram;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::MaybeOwnedRow,
    pattern_executor::PatternExecutor,
    pipeline::{
        PipelineError,
        StageAPI,
    },
};
use crate::pipeline::StageIterator;

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

        let iterator = previous_iterator.flat_map(move |row| {
            // TODO: use `row` as input into the executor
            let executor = PatternExecutor::new(program.entry(), snapshot.as_ref(), thing_manager.as_ref())
                .map_err(|source| PipelineError::InitialisingMatchIterator(source))?;
            executor.into_iterator(snapshot.clone(), thing_manager.clone())
        });
        Ok(MatchStageIterator::new(iterator))
    }
}

pub struct MatchStageIterator<Iterator> {
    iterator: Iterator,
}

impl<Iterator> MatchStageIterator<Iterator>
    where
        Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>
{
    fn new(iterator: Iterator) -> Self {
        Self { iterator }
    }
}

impl<Iterator> LendingIterator for MatchStageIterator<Iterator>
    where
        Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

impl<Iterator> StageIterator for MatchStageIterator<Iterator>
where
    Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>
{}