/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use compiler::match_::planner::program_plan::ProgramPlan;
use concept::{error::ConceptReadError};
use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, ImmutableRow},
    pattern_executor::{BatchIterator, PatternExecutor},
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI},
};

pub struct MatchStage<Snapshot: ReadableSnapshot + 'static, PipelineStageType: PipelineStageAPI<Snapshot>> {
    inner: Option<Either<LazyMatchStage<Snapshot, PipelineStageType>, MatchStageIterator<Snapshot>>>, // TODO: Figure out how to neatly turn one into the other
    error: Option<PipelineError>,
}
impl<Snapshot: ReadableSnapshot + 'static, PipelineStageType: PipelineStageAPI<Snapshot>>
    MatchStage<Snapshot, PipelineStageType>
{
    pub fn new(upstream: Box<PipelineStageType>, program_plan: ProgramPlan) -> Self {
        Self { inner: Some(Either::Left(LazyMatchStage::new(upstream, program_plan))), error: None }
    }
}
impl<Snapshot: ReadableSnapshot, PipelineStage: PipelineStageAPI<Snapshot>> LendingIterator
    for MatchStage<Snapshot, PipelineStage>
{
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.inner.is_some() && self.inner.as_ref().unwrap().is_left() {
            let Either::Left(lazy_stage) = self.inner.take().unwrap() else { unreachable!() };
            let LazyMatchStage { upstream, program_plan, .. } = lazy_stage;
            let (snapshot, thing_manager) = match upstream.finalise() {
                PipelineContext::Arced(snapshot, thing_manager) => (snapshot, thing_manager),
                PipelineContext::Owned(snapshot, thing_manager) => (Arc::new(snapshot), Arc::new(thing_manager)),
            };
            let snapshot_borrowed: &Snapshot = &snapshot;
            match PatternExecutor::new(program_plan.entry(), snapshot_borrowed, &thing_manager) {
                Ok(executor) => {
                    let batch_iterator = BatchIterator::new(executor, snapshot, thing_manager);
                    let match_iterator = MatchStageIterator::new(batch_iterator);
                    self.inner = Some(Either::Right(match_iterator))
                }
                Err(error) => {
                    self.error = Some(PipelineError::ConceptRead(error));
                }
            }
        };

        if self.error.is_some() {
            Some(Err(self.error.as_ref().unwrap().clone()))
        } else {
            debug_assert!(self.inner.is_some() && self.inner.as_ref().unwrap().is_right());
            let Either::Right(iterator) = self.inner.as_mut().unwrap() else { unreachable!() };
            iterator.next()
        }
    }
}

impl<Snapshot: ReadableSnapshot, PipelineStageType: PipelineStageAPI<Snapshot>> PipelineStageAPI<Snapshot>
    for MatchStage<Snapshot, PipelineStageType>
{
    fn finalise(self) -> PipelineContext<Snapshot> {
        match self.inner {
            Some(Either::Left(lazy)) => todo!("Illegal, but unhandled"),
            Some(Either::Right(iterator)) => {
                // TODO: If we're here, we should have pulled atleast once, right?
                iterator.renameme__finalise()
            }
            None => todo!("Illegal again, but I don't prevent it?"),
        }
    }
}

pub struct LazyMatchStage<Snapshot: ReadableSnapshot, PipelineStageType: PipelineStageAPI<Snapshot>> {
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

pub struct MatchStageIterator<Snapshot: ReadableSnapshot> {
    batch_iterator: BatchIterator<Snapshot>,
    current_batch: Option<Result<Batch, ConceptReadError>>,
    current_index: u32,
}

impl<Snapshot: ReadableSnapshot> MatchStageIterator<Snapshot> {
    fn renameme__finalise(self) -> PipelineContext<Snapshot> {
        let (_, context) = self.batch_iterator.into_parts();
        context
    }
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
