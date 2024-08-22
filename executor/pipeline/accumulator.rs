/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    pipeline::{IteratingStageAPI, PipelineContext, PipelineError, PipelineStageAPI, UninitialisedStageAPI},
};

pub(crate) trait AccumulatingStageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    fn process_accumulated(
        &self,
        context: &mut PipelineContext<Snapshot>,
        row: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError>;

    fn store_incoming_row_into(&self, incoming: &ImmutableRow<'_>, stored_row: &mut Box<[VariableValue<'static>]>) {
        (0..incoming.width()).for_each(|i| {
            stored_row[i] = incoming.get(VariablePosition::new(i as u32)).clone().into_owned();
        });
    }

    fn must_deduplicate_incoming_rows(&self) -> bool;
    fn row_width(&self) -> usize;
}

// TODO: Optimise for allocations
pub struct Accumulator<Snapshot, PipelineStageType, Executor>
where
    Snapshot: ReadableSnapshot + 'static,
    Executor: AccumulatingStageAPI<Snapshot>,
    PipelineStageType: PipelineStageAPI<Snapshot>,
{
    upstream: Box<PipelineStageType>,
    rows: Vec<(Box<[VariableValue<'static>]>, u64)>,
    executor: Executor,
    phantom: PhantomData<Snapshot>,
}

impl<PipelineStageType, Snapshot, Executor> Accumulator<Snapshot, PipelineStageType, Executor>
where
    Snapshot: ReadableSnapshot + 'static,
    Executor: AccumulatingStageAPI<Snapshot>,
    PipelineStageType: PipelineStageAPI<Snapshot>,
{
    pub(crate) fn new(upstream: Box<PipelineStageType>, executor: Executor) -> Self {
        Self { upstream, executor, rows: Vec::new(), phantom: PhantomData }
    }

    fn accumulate(&mut self) -> Result<(), PipelineError> {
        let Self { executor, rows, upstream, .. } = self;
        while let Some(result) = upstream.next() {
            match result {
                Err(err) => return Err(err),
                Ok(row) => {
                    Self::accept_incoming_row(rows, executor, row);
                }
            }
        }
        Ok(())
    }

    fn accept_incoming_row(
        rows: &mut Vec<(Box<[VariableValue<'static>]>, u64)>,
        executor: &mut Executor,
        incoming: ImmutableRow<'_>,
    ) {
        let (output_multiplicity, output_row_count) = if executor.must_deduplicate_incoming_rows() {
            (1, incoming.get_multiplicity())
        } else {
            (incoming.get_multiplicity(), 1)
        };
        for _ in 0..output_row_count {
            let mut stored_row =
                (0..executor.row_width()).map(|_| VariableValue::Empty).collect::<Vec<_>>().into_boxed_slice();
            executor.store_incoming_row_into(&incoming, &mut stored_row);
            rows.push((stored_row, output_multiplicity));
        }
    }

    pub fn accumulate_process_and_into_iterator(mut self) -> Result<AccumulatedRowIterator<Snapshot>, PipelineError> {
        self.accumulate()?;
        let Self { executor, rows, upstream, .. } = self;
        let mut context = upstream.finalise_and_into_context()?; // TODO: Need not always be owned
        let mut rows = rows.into_boxed_slice();
        executor.process_accumulated(&mut context, &mut rows)?;
        Ok(AccumulatedRowIterator { context, rows, next_index: 0 })
    }
}

impl<
        Snapshot: ReadableSnapshot + 'static,
        PipelineStageType: PipelineStageAPI<Snapshot>,
        Executor: AccumulatingStageAPI<Snapshot>,
    > UninitialisedStageAPI<Snapshot> for Accumulator<Snapshot, PipelineStageType, Executor>
{
    type IteratingStage = AccumulatedRowIterator<Snapshot>;

    fn initialise_and_into_iterator(mut self) -> Result<Self::IteratingStage, PipelineError> {
        self.upstream.initialise()?;
        self.accumulate_process_and_into_iterator()
    }
}

pub struct AccumulatedRowIterator<Snapshot: ReadableSnapshot + 'static> {
    context: PipelineContext<Snapshot>,
    rows: Box<[(Box<[VariableValue<'static>]>, u64)]>,
    next_index: usize,
}

impl<Snapshot: ReadableSnapshot + 'static> IteratingStageAPI<Snapshot> for AccumulatedRowIterator<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        self.context.try_get_shared()
    }

    fn finalise_and_into_context(mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        // TODO: Ensure we have been consumed
        if self.next().is_some() {
            Err(PipelineError::IllegalState)
        } else {
            Ok(self.context)
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for AccumulatedRowIterator<Snapshot> {
    // type Item<'a> = Result<ImmutableRow<'a>, Executor::Error>;
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;
    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.next_index < self.rows.len() {
            let (row, multiplicity) = self.rows.get(self.next_index).unwrap();
            self.next_index += 1;
            Some(Ok(ImmutableRow::new(row, *multiplicity)))
        } else {
            None
        }
    }
}
