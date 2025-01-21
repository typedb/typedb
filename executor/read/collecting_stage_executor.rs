/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, iter::Peekable, sync::Arc};

use compiler::executable::{modifiers::SortExecutable, reduce::ReduceRowsExecutable};
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

pub(crate) struct CollectingStageExecutor {
    pattern: PatternExecutor,
    collector: CollectorEnum,
}

pub(super) enum CollectorEnum {
    Reduce(ReduceCollector),
    Sort(SortCollector),
}

impl CollectorEnum {
    pub(crate) fn accept(&mut self, context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch) {
        match self {
            CollectorEnum::Reduce(collector) => collector.accept(context, batch),
            CollectorEnum::Sort(collector) => collector.accept(context, batch),
        }
    }

    pub(crate) fn collected_to_iterator(&mut self) -> CollectedStageIterator {
        match self {
            CollectorEnum::Reduce(collector) => collector.collected_to_iterator(),
            CollectorEnum::Sort(collector) => collector.collected_to_iterator(),
        }
    }
}

pub(super) enum CollectedStageIterator {
    Reduce(ReduceStageIterator),
    Sort(SortStageIterator),
}

impl CollectedStageIterator {
    pub(crate) fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            CollectedStageIterator::Reduce(iterator) => iterator.batch_continue(),
            CollectedStageIterator::Sort(iterator) => iterator.batch_continue(),
        }
    }
}

impl CollectingStageExecutor {
    pub(super) fn to_parts_mut(&mut self) -> (&mut PatternExecutor, &mut CollectorEnum) {
        let Self { pattern, collector } = self;
        (pattern, collector)
    }

    pub(crate) fn new_reduce(
        previous_stage: PatternExecutor,
        reduce_rows_executable: Arc<ReduceRowsExecutable>,
    ) -> Self {
        Self { pattern: previous_stage, collector: CollectorEnum::Reduce(ReduceCollector::new(reduce_rows_executable)) }
    }

    pub(crate) fn new_sort(previous_stage: PatternExecutor, sort_executable: &SortExecutable) -> Self {
        Self { pattern: previous_stage, collector: CollectorEnum::Sort(SortCollector::new(sort_executable)) }
    }

    pub(crate) fn reset(&mut self) {
        self.pattern.reset();
        match &mut self.collector {
            CollectorEnum::Reduce(collector) => collector.reset(),
            CollectorEnum::Sort(collector) => collector.reset(),
        }
    }

    pub(crate) fn prepare(&mut self, batch: FixedBatch) {
        debug_assert!({
            match &self.collector {
                CollectorEnum::Reduce(_) => batch.len() == 1,
                _ => true,
            }
        });
        self.pattern.prepare(batch);
        match &mut self.collector {
            CollectorEnum::Reduce(collector) => collector.prepare(),
            CollectorEnum::Sort(collector) => collector.prepare(),
        }
    }
}

pub(super) trait CollectorTrait {
    fn prepare(&mut self);
    fn reset(&mut self);
    fn accept(&mut self, context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch);
    fn collected_to_iterator(&mut self) -> CollectedStageIterator;
}

pub(super) trait CollectedStageIteratorTrait {
    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError>;
}

// Reduce
pub(super) struct ReduceCollector {
    reduce_executable: Arc<ReduceRowsExecutable>,
    active_reducer: Option<GroupedReducer>,
    output: Option<BatchRowIterator>,
    output_width: u32,
}

impl ReduceCollector {
    fn new(reduce_executable: Arc<ReduceRowsExecutable>) -> Self {
        let output_width = (reduce_executable.input_group_positions.len() + reduce_executable.reductions.len()) as u32;
        Self { reduce_executable, active_reducer: None, output: None, output_width }
    }
}

impl CollectorTrait for ReduceCollector {
    fn prepare(&mut self) {
        self.active_reducer = Some(GroupedReducer::new(self.reduce_executable.clone()));
    }

    fn reset(&mut self) {
        self.active_reducer = None;
    }

    fn accept(&mut self, context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch) {
        let active_reducer = self.active_reducer.as_mut().unwrap();
        for row in batch {
            active_reducer.accept(&row, context).unwrap(); // TODO: potentially unsafe unwrap
        }
    }

    fn collected_to_iterator(&mut self) -> CollectedStageIterator {
        CollectedStageIterator::Reduce(ReduceStageIterator::new(
            self.active_reducer.take().unwrap().finalise().into_iterator(),
            self.output_width,
        ))
    }
}

pub(super) struct ReduceStageIterator {
    batch_row_iterator: BatchRowIterator,
    output_width: u32,
}

impl ReduceStageIterator {
    fn new(batch: BatchRowIterator, output_width: u32) -> Self {
        Self { batch_row_iterator: batch, output_width }
    }
}

impl CollectedStageIteratorTrait for ReduceStageIterator {
    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let mut next_batch = FixedBatch::new(self.output_width);
        while !next_batch.is_full() {
            if let Some(row) = self.batch_row_iterator.next() {
                next_batch.append(|mut output_row| output_row.copy_from_row(row));
            } else {
                break;
            }
        }
        if !next_batch.is_empty() {
            Ok(Some(next_batch))
        } else {
            Ok(None)
        }
    }
}

// Sort
pub(super) struct SortCollector {
    sort_on: Vec<(usize, bool)>,
    collector: Option<Batch>,
}

impl SortCollector {
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
        Self { sort_on, collector: None }
    }
}

impl CollectorTrait for SortCollector {
    fn prepare(&mut self) {
        // self.collector = Some(Batch::new(self.output_width));
    }

    fn reset(&mut self) {
        self.collector = None;
    }

    fn accept(&mut self, _context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch) {
        for row in batch {
            if self.collector.is_none() {
                self.collector = Some(Batch::new(row.len() as u32, 0usize))
            }
            self.collector.as_mut().unwrap().append(row);
        }
    }

    fn collected_to_iterator(&mut self) -> CollectedStageIterator {
        let unsorted = self.collector.take().unwrap();
        let mut indices: Vec<usize> = (0..unsorted.len()).collect();
        indices.sort_by(|x, y| {
            let x_row_as_row = unsorted.get_row(*x);
            let y_row_as_row = unsorted.get_row(*y);
            let x_row = x_row_as_row.row();
            let y_row = y_row_as_row.row();
            // TODO: We need to be sorting on values of attributes.
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
        let sorted_indices = indices.into_iter().peekable();
        CollectedStageIterator::Sort(SortStageIterator { unsorted, sorted_indices })
    }
}

pub struct SortStageIterator {
    unsorted: Batch,
    sorted_indices: Peekable<std::vec::IntoIter<usize>>,
}

impl CollectedStageIteratorTrait for SortStageIterator {
    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let Self { unsorted, sorted_indices } = self;
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
            Ok(None)
        }
    }
}
