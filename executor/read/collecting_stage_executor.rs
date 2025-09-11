/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, iter::Peekable, sync::Arc};

use compiler::executable::{modifiers::SortExecutable, reduce::ReduceRowsExecutable};
use ir::pipeline::modifier::SortVariable;
use lending_iterator::LendingIterator;
use resource::constants::traversal::BATCH_DEFAULT_CAPACITY;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, BatchRowIterator, FixedBatch},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::pattern_executor::PatternExecutor,
    reduce_executor::GroupedReducer,
};

#[derive(Debug)]
pub(crate) enum CollectingStageExecutor {
    Reduce { pattern: PatternExecutor, reduce_rows_executable: Arc<ReduceRowsExecutable> },
    Sort { pattern: PatternExecutor, sort_on: Arc<Vec<(usize, bool)>> },
}

impl CollectingStageExecutor {
    pub(crate) fn new_reduce(
        previous_stage: PatternExecutor,
        reduce_rows_executable: Arc<ReduceRowsExecutable>,
    ) -> Self {
        Self::Reduce { pattern: previous_stage, reduce_rows_executable }
    }

    pub(crate) fn new_sort(previous_stage: PatternExecutor, sort_executable: &SortExecutable) -> Self {
        let sort_on = sort_executable
            .sort_on
            .iter()
            .map(|sort_variable| match sort_variable {
                SortVariable::Ascending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), true),
                SortVariable::Descending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), false),
            })
            .collect();
        Self::Sort { pattern: previous_stage, sort_on: Arc::new(sort_on) }
    }

    pub(crate) fn output_width(&self) -> u32 {
        match self {
            CollectingStageExecutor::Reduce { pattern, .. }
            | CollectingStageExecutor::Sort { pattern, .. } => pattern.output_width(),
        }
    }

    pub(super) fn pattern_mut(&mut self) -> &mut PatternExecutor {
        match self {
            CollectingStageExecutor::Reduce { pattern, .. } => pattern,
            CollectingStageExecutor::Sort { pattern, .. } => pattern,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.pattern_mut().reset()
    }

    pub(crate) fn prepare(&mut self, batch: FixedBatch) {
        debug_assert!({
            match self {
                Self::Reduce { .. } => batch.len() == 1,
                _ => true,
            }
        });
        self.pattern_mut().prepare(batch)
    }

    pub(crate) fn create_collector(&self) -> CollectorEnum {
        match self {
            CollectingStageExecutor::Reduce { reduce_rows_executable, .. } => {
                CollectorEnum::Reduce(ReduceCollector::new(reduce_rows_executable.clone()))
            }
            CollectingStageExecutor::Sort { sort_on, pattern } => {
                CollectorEnum::Sort(SortCollector::new(pattern.output_width(), sort_on.clone()))
            },
        }
    }
}

#[derive(Debug)]
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

    pub(crate) fn into_iterator(self, context: &ExecutionContext<impl ReadableSnapshot>) -> CollectedStageIterator {
        match self {
            CollectorEnum::Reduce(collector) => collector.into_iterator(context),
            CollectorEnum::Sort(collector) => collector.into_iterator(context),
        }
    }
}

#[derive(Debug)]
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

// Actual implementations
pub(super) trait CollectorTrait {
    fn accept(&mut self, context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch);
    fn into_iterator(self, context: &ExecutionContext<impl ReadableSnapshot>) -> CollectedStageIterator;
}

pub(super) trait CollectedStageIteratorTrait {
    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError>;
}

// Reduce
pub(super) struct ReduceCollector {
    active_reducer: GroupedReducer,
    output_width: u32,
}

impl fmt::Debug for ReduceCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReduceCollector")
    }
}

impl ReduceCollector {
    fn new(reduce_executable: Arc<ReduceRowsExecutable>) -> Self {
        let output_width = (reduce_executable.input_group_positions.len() + reduce_executable.reductions.len()) as u32;
        Self { active_reducer: GroupedReducer::new(reduce_executable), output_width }
    }
}

impl CollectorTrait for ReduceCollector {
    fn accept(&mut self, context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch) {
        for row in batch {
            self.active_reducer.accept(&row, context).unwrap(); // TODO: potentially unsafe unwrap
        }
    }

    fn into_iterator(self, _context: &ExecutionContext<impl ReadableSnapshot>) -> CollectedStageIterator {
        CollectedStageIterator::Reduce(ReduceStageIterator::new(
            self.active_reducer.finalise().into_iterator(),
            self.output_width,
        ))
    }
}

#[derive(Debug)]
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
#[derive(Debug)]
pub(super) struct SortCollector {
    sort_on: Arc<Vec<(usize, bool)>>,
    collector: Batch,
}

impl SortCollector {
    fn new(width: u32, sort_on: Arc<Vec<(usize, bool)>>) -> Self {
        // let output_width = sort_executable.output_width;  // TODO: Get this information into the sort_executable.
        Self { sort_on, collector: Batch::new(width, BATCH_DEFAULT_CAPACITY) }
    }
}

impl CollectorTrait for SortCollector {
    fn accept(&mut self, _context: &ExecutionContext<impl ReadableSnapshot>, batch: FixedBatch) {
        for row in batch {
            self.collector.append_row(row);
        }
    }

    fn into_iterator(self, context: &ExecutionContext<impl ReadableSnapshot>) -> CollectedStageIterator {
        let Self { sort_on, collector } = self;
        let profile = context.profile.profile_stage(|| String::from("Sort"), 0); // TODO executable id
        let step_profile = profile.extend_or_get(0, || String::from("Sort execution"));
        let sorted_indices =
            collector.indices_sorted_by(context, &sort_on, step_profile.storage_counters()).into_iter().peekable();
        CollectedStageIterator::Sort(SortStageIterator { unsorted: collector, sorted_indices })
    }
}

#[derive(Debug)]
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
