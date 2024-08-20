/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::ImmutableRow,
    pipeline::{
        accumulator::{AccumulatedRowIterator, Accumulator},
        PipelineContext, PipelineError, PipelineStageAPI, WritablePipelineStage,
    },
    write::insert::InsertExecutor,
};

type InsertAccumulator<Snapshot: WritableSnapshot + 'static> =
    Accumulator<Snapshot, WritablePipelineStage<Snapshot>, InsertExecutor>;
pub struct InsertStage<Snapshot: WritableSnapshot + 'static> {
    inner: Option<Either<InsertAccumulator<Snapshot>, AccumulatedRowIterator<Snapshot>>>, // TODO: Figure out how to neatly turn one into the other
    error: Option<PipelineError>,
}

impl<Snapshot: WritableSnapshot + 'static> InsertStage<Snapshot> {
    pub fn new(upstream: Box<WritablePipelineStage<Snapshot>>, executor: InsertExecutor) -> InsertStage<Snapshot> {
        let accumulator = Accumulator::new(upstream, executor);
        Self { inner: Some(Either::Left(accumulator)), error: None }
    }
}

impl<Snapshot: WritableSnapshot> LendingIterator for InsertStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.inner.is_some() && self.inner.as_ref().unwrap().is_left() {
            let Either::Left(accumulator) = self.inner.take().unwrap() else { unreachable!() };
            match accumulator.accumulate_process_and_iterate() {
                Ok(iterator) => self.inner = Some(Either::Right(iterator)),
                Err(err) => {
                    self.error = Some(err);
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

impl<Snapshot: WritableSnapshot> PipelineStageAPI<Snapshot> for InsertStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        match self.inner {
            Some(Either::Left(accumulator)) => todo!("Illegal, but unhandled"),
            Some(Either::Right(iterator)) => iterator.finalise(),
            None => todo!("Illegal again, but I don't prevent it?"),
        }
    }
}
