/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use compiler::match_::planner::program_plan::ProgramPlan;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, ImmutableRow},
    pattern_executor::{BatchIterator, PatternExecutor},
    pipeline::{
        common::PipelineStageCommon, IteratingStageAPI, PipelineContext, PipelineError, PipelineStageAPI,
        UninitialisedStageAPI,
    },
};

pub type MatchStage<Snapshot: ReadableSnapshot, PipelineStageType: PipelineStageAPI<Snapshot>> = PipelineStageCommon<
    Snapshot,
    PipelineStageType,
    LazyMatchStage<Snapshot, PipelineStageType>,
    MatchStageIterator<Snapshot>,
>;

impl<Snapshot: ReadableSnapshot + 'static, PipelineStageType: PipelineStageAPI<Snapshot>>
    MatchStage<Snapshot, PipelineStageType>
{
    pub fn new(upstream: Box<PipelineStageType>, program_plan: ProgramPlan) -> Self {
        Self::new_impl(LazyMatchStage::new(upstream, program_plan))
    }
}

pub struct LazyMatchStage<Snapshot: ReadableSnapshot + 'static, PipelineStageType: PipelineStageAPI<Snapshot>> {
    program_plan: ProgramPlan,
    upstream: Box<PipelineStageType>,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot + 'static, PipelineStageType: PipelineStageAPI<Snapshot>>
    LazyMatchStage<Snapshot, PipelineStageType>
{
    pub fn new(upstream: Box<PipelineStageType>, program_plan: ProgramPlan) -> Self {
        Self { program_plan, upstream, phantom: PhantomData }
    }
}

impl<Snapshot: ReadableSnapshot, PipelineStageType: PipelineStageAPI<Snapshot>> UninitialisedStageAPI<Snapshot>
    for LazyMatchStage<Snapshot, PipelineStageType>
{
    type IteratingStage = MatchStageIterator<Snapshot>;

    fn initialise_and_into_iterator(mut self) -> Result<Self::IteratingStage, PipelineError> {
        self.upstream.initialise()?;
        let LazyMatchStage { mut upstream, program_plan, .. } = self;
        let mut context = upstream.try_get_shared_context()?;
        let (snapshot_borrowed, thing_manager_borrowed): (&Snapshot, &ThingManager) = context.borrow_parts();
        let executor = PatternExecutor::new(program_plan.entry(), snapshot_borrowed, &thing_manager_borrowed)
            .map_err(|source| PipelineError::ConceptRead(source))?;
        let PipelineContext::Shared(shared_snapshot, shared_thing_manager) = context.try_get_shared()? else {
            unreachable!()
        };
        let batch_iterator = BatchIterator::new(executor, shared_snapshot, shared_thing_manager);
        Ok(MatchStageIterator::new(batch_iterator))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot> {
    batch_iterator: BatchIterator<Snapshot>,
    current_batch: Option<Result<Batch, ConceptReadError>>,
    current_index: u32,
}

impl<Snapshot: ReadableSnapshot + 'static> MatchStageIterator<Snapshot> {
    fn new(batch_iterator: BatchIterator<Snapshot>) -> Self {
        Self { batch_iterator, current_batch: Some(Ok(Batch::EMPTY)), current_index: 0 }
    }

    fn forward_batches_till_has_next_or_none(&mut self) {
        let must_fetch_next = match &self.current_batch {
            None => false,
            Some(Err(_)) => false,
            Some(Ok(batch)) => self.current_index >= batch.rows_count(),
        };
        if must_fetch_next {
            self.current_batch = self.batch_iterator.next();
            self.forward_batches_till_has_next_or_none(); // Just in case we have empty batches
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for MatchStageIterator<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.forward_batches_till_has_next_or_none();
        match &self.current_batch {
            None => None,
            Some(Err(err)) => Some(Err(PipelineError::ConceptRead(err.clone()))),
            Some(Ok(batch)) => {
                self.current_index += 1;
                Some(Ok(batch.get_row(self.current_index - 1)))
            }
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> IteratingStageAPI<Snapshot> for MatchStageIterator<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        self.batch_iterator.try_get_shared_context()
    }

    fn finalise_and_into_context(mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        if self.next().is_some() {
            Err(PipelineError::FinalisedUnconsumedStage)
        } else {
            let (_, context) = self.batch_iterator.into_parts();
            Ok(context)
        }
    }
}
