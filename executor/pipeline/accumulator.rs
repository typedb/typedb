/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use answer::variable_value::VariableValue;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::ImmutableRow,
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI},
};

// TODO: Optimise for allocations
pub(crate) struct Accumulator<Snapshot, PipelineStageType, Executor>
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

    pub fn accumulate_process_and_iterate(mut self) -> Result<AccumulatedRowIterator<Snapshot>, PipelineError> {
        self.accumulate()?;
        let Self { executor, rows, upstream, .. } = self;
        let mut context = upstream.finalise();
        let mut rows = rows.into_boxed_slice();
        executor.process_accumulated(&mut context, &mut rows)?;
        Ok(AccumulatedRowIterator { context, rows, next_index: 0 })
    }
}

pub(crate) trait AccumulatingStageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    fn process_accumulated(
        &self,
        context: &mut PipelineContext<Snapshot>,
        row: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError>;
    fn store_incoming_row_into(&self, incoming: &ImmutableRow<'_>, stored_row: &mut Box<[VariableValue<'static>]>);
    fn must_deduplicate_incoming_rows(&self) -> bool;
    fn row_width(&self) -> usize;
}

pub struct AccumulatedRowIterator<Snapshot: ReadableSnapshot + 'static> {
    context: PipelineContext<Snapshot>,
    rows: Box<[(Box<[VariableValue<'static>]>, u64)]>,
    next_index: usize,
}

impl<Snapshot: ReadableSnapshot + 'static> AccumulatedRowIterator<Snapshot> {
    pub(crate) fn finalise(self) -> PipelineContext<Snapshot> {
        // TODO: Ensure we have been consumed
        debug_assert!(self.next_index >= self.rows.len());
        self.context
    }
}

// TODO: Implement LendingIterator instead ?
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
