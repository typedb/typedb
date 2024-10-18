/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, iter::Peekable};

use compiler::executable::{modifiers::SortExecutable, reduce::ReduceExecutable};
use ir::pipeline::modifier::SortVariable;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, BatchRowIterator, FixedBatch},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::pattern_executor::PatternExecutor,
    reduce_executor::GroupedReducer,
};

pub(super) enum CollectingStageExecutor {
    Reduce(PatternExecutor, ReduceController),
    Sort(PatternExecutor, SortController),
}

impl CollectingStageExecutor {
    pub(crate) fn new_reduce(previous_stage: PatternExecutor, reduce_executable: &ReduceExecutable) -> Self {
        Self::Reduce(previous_stage, ReduceController::new(reduce_executable))
    }

    pub(crate) fn new_sort(previous_stage: PatternExecutor, sort_executable: &SortExecutable) -> Self {
        Self::Sort(previous_stage, SortController::new(sort_executable))
    }

    pub(crate) fn reset(&mut self) {
        match self {
            CollectingStageExecutor::Reduce(inner, controller) => {
                inner.reset();
                controller.reset()
            }
            CollectingStageExecutor::Sort(inner, controller) => {
                inner.reset();
                controller.reset();
            }
        }
    }
    pub(crate) fn prepare(&mut self, batch: FixedBatch) {
        debug_assert!(batch.len() == 1);
        match self {
            CollectingStageExecutor::Reduce(inner, controller) => {
                inner.prepare(batch);
                controller.prepare();
            }
            CollectingStageExecutor::Sort(inner, controller) => {
                inner.prepare(batch);
                controller.prepare();
            }
        }
    }
}

pub(super) enum CollectingStageState {
    Inactive,
    Collecting,
    Streaming,
}

pub(super) trait CollectingStageController {
    fn prepare(&mut self);
    fn reset(&mut self);
    fn accept(&mut self, batch: FixedBatch, context: &ExecutionContext<impl ReadableSnapshot>);
    fn transform(&mut self);
    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError>;
}

pub(super) struct ReduceController {
    reduce_executable: ReduceExecutable,
    active_reducer: Option<GroupedReducer>,
    output: Option<BatchRowIterator>,
    output_width: u32,
}

impl ReduceController {
    fn new(reduce_executable: &ReduceExecutable) -> Self {
        let output_width = (reduce_executable.input_group_positions.len() + reduce_executable.reductions.len()) as u32;
        Self { reduce_executable: reduce_executable.clone(), active_reducer: None, output: None, output_width }
    }
}

impl CollectingStageController for ReduceController {
    fn prepare(&mut self) {
        self.active_reducer = Some(GroupedReducer::new(self.reduce_executable.clone()));
        self.output = None;
    }

    fn reset(&mut self) {
        self.active_reducer = None;
        self.output = None;
    }

    fn accept(&mut self, batch: FixedBatch, context: &ExecutionContext<impl ReadableSnapshot>) {
        let active_reducer = self.active_reducer.as_mut().unwrap();
        let mut batch_iter = batch.into_iterator();
        while let Some(row) = batch_iter.next() {
            active_reducer.accept(&row.unwrap(), context).unwrap(); // TODO: potentially unsafe unwrap
        }
    }

    fn transform(&mut self) {
        self.output = Some(self.active_reducer.take().unwrap().finalise().into_iterator());
    }

    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let Some(output) = &mut self.output else {
            unreachable!();
        };
        let mut next_batch = FixedBatch::new(self.output_width);
        while !next_batch.is_full() {
            if let Some(row) = output.next() {
                next_batch.append(|mut output_row| {
                    output_row.copy_from(row.row(), row.multiplicity());
                })
            } else {
                break;
            }
        }
        if next_batch.len() > 0 {
            Ok(Some(next_batch))
        } else {
            Ok(None)
        }
    }
}

pub(super) struct SortController {
    sort_on: Vec<(usize, bool)>,
    collector: Option<Batch>,
    sorted_indices: Option<Peekable<std::vec::IntoIter<usize>>>,
}

impl SortController {
    fn new(sort_executable: &SortExecutable) -> Self {
        let sort_on = sort_executable
            .sort_on
            .iter()
            .map(|sort_variable| match sort_variable {
                SortVariable::Ascending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), true),
                SortVariable::Descending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), false),
            })
            .collect();
        // let output_width = sort_executable.output_width;  // TODO: Get this information into the sort_executable.
        Self { sort_on, collector: None, sorted_indices: None }
    }
}

impl CollectingStageController for SortController {
    fn prepare(&mut self) {
        // self.collector = Some(Batch::new(self.output_width));
    }

    fn reset(&mut self) {
        self.collector = None;
    }

    fn accept(&mut self, batch: FixedBatch, _: &ExecutionContext<impl ReadableSnapshot>) {
        let mut batch_iter = batch.into_iterator();
        while let Some(result) = batch_iter.next() {
            let row = result.unwrap();
            if self.collector.is_none() {
                self.collector = Some(Batch::new(row.len() as u32, 0 as usize)) // TODO: Remove this workaround once we have output_width
            }
            self.collector.as_mut().unwrap().append(row);
        }
    }

    fn transform(&mut self) {
        let unsorted = self.collector.as_mut().unwrap();
        let mut indices: Vec<usize> = (0..unsorted.len()).collect();
        indices.sort_by(|x, y| {
            let x_row_as_row = unsorted.get_row(*x);
            let y_row_as_row = unsorted.get_row(*y);
            let x_row = x_row_as_row.row();
            let y_row = y_row_as_row.row();
            for (idx, asc) in &self.sort_on {
                let ord = x_row[*idx]
                    .partial_cmp(&y_row[*idx])
                    .expect("Sort on variable with uncomparable values should have been caught at query-compile time");
                match (asc, ord) {
                    (true, Ordering::Less) | (false, Ordering::Greater) => return Ordering::Less,
                    (true, Ordering::Greater) | (false, Ordering::Less) => return Ordering::Greater,
                    (true, Ordering::Equal) | (false, Ordering::Equal) => {}
                };
            }
            Ordering::Equal
        });
        self.sorted_indices = Some(indices.into_iter().peekable());
    }

    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let Self { collector: Some(unsorted), sorted_indices: Some(sorted_indices), .. } = self else { unreachable!() };
        if sorted_indices.peek().is_some() {
            let width = unsorted.get_row(0).len();
            let mut next_batch = FixedBatch::new(width as u32);
            while !next_batch.is_full() && sorted_indices.peek().is_some() {
                let index = sorted_indices.next().unwrap();
                next_batch.append(|mut copy_to_row| {
                    copy_to_row.copy_from_row(unsorted.get_row(index)); // TODO: Can we avoid a copy?
                });
            }
            Ok(Some(next_batch))
        } else {
            return Ok(None);
        }
    }
}
