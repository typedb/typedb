/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{cmp::Ordering, collections::HashMap, marker::PhantomData, sync::Arc};

use compiler::{
    modifiers::{FilterProgram, LimitProgram, OffsetProgram, SortProgram},
    VariablePosition,
};
use ir::program::modifier::SortVariable;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;
use crate::{
    batch::Batch,
    ExecutionInterrupt,
    pipeline::{PipelineExecutionError, StageAPI, StageIterator},
    row::MaybeOwnedRow,
};

// Sort
pub struct SortStageExecutor<Snapshot, PreviousStage> {
    program: SortProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> SortStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(program: SortProgram, previous: PreviousStage) -> Self {
        Self { program, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for SortStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = SortStageIterator<Snapshot>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        // TODO: Do we really need this function apart from at  the final stage? If so, can't we make it a property of the pipeline instead?
        self.previous.named_selected_outputs()
    }

    fn into_iterator(self, interrupt: ExecutionInterrupt) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let Self { previous, program, .. } = self;
        let (previous_iterator, mut snapshot) = previous.into_iterator(interrupt)?;
        // accumulate once, then we will operate in-place
        let batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((snapshot, err)),
        };
        Ok((SortStageIterator::from_unsorted(batch, program), snapshot))
    }
}

pub struct SortStageIterator<Snapshot>
where
    Snapshot: ReadableSnapshot + 'static,
{
    unsorted: Batch,
    sorted_indices: Box<[usize]>,
    next_index_index: usize,
    phantom: PhantomData<Snapshot>,
}
impl<Snapshot> SortStageIterator<Snapshot>
where
    Snapshot: ReadableSnapshot + 'static,
{
    fn from_unsorted(unsorted: Batch, sort_program: SortProgram) -> Self {
        let mut indices: Vec<usize> = (0..unsorted.len()).collect();
        let sort_by: Vec<(usize, bool)> = sort_program
            .sort_on
            .iter()
            .map(|sort_variable| match sort_variable {
                SortVariable::Ascending(v) => (sort_program.output_row_mapping.get(v).unwrap().as_usize(), true),
                SortVariable::Descending(v) => (sort_program.output_row_mapping.get(v).unwrap().as_usize(), false),
            })
            .collect();
        indices.sort_by(|x, y| {
            let x_row_as_row = unsorted.get_row(*x);
            let y_row_as_row = unsorted.get_row(*y);
            let x_row = x_row_as_row.get_row();
            let y_row = y_row_as_row.get_row();
            for (idx, asc) in &sort_by {
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
        Self {
            unsorted,
            sorted_indices: indices.into_boxed_slice(),
            next_index_index: 0,
            phantom: PhantomData::default(),
        }
    }
}

impl<Snapshot> LendingIterator for SortStageIterator<Snapshot>
where
    Snapshot: ReadableSnapshot + 'static,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.next_index_index < self.unsorted.len() {
            let row = self.unsorted.get_row(self.sorted_indices[self.next_index_index]);
            self.next_index_index += 1;
            Some(Ok(row))
        } else {
            None
        }
    }
}

impl<Snapshot> StageIterator for SortStageIterator<Snapshot> where Snapshot: ReadableSnapshot + 'static {}

// Offset
pub struct OffsetStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    offset_program: OffsetProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> OffsetStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(offset_program: OffsetProgram, previous: PreviousStage) -> Self {
        Self { offset_program, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for OffsetStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = OffsetStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        self.previous.named_selected_outputs()
    }

    fn into_iterator(self, interrupt: ExecutionInterrupt) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let Self { offset_program, previous, .. } = self;
        let (previous_iterator, snapshot) = previous.into_iterator(interrupt)?;
        Ok((OffsetStageIterator::new(previous_iterator, offset_program.offset), snapshot))
    }
}

pub struct OffsetStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    remaining: u64,
    previous: PreviousIterator,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousIterator> OffsetStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    fn new(previous: PreviousIterator, offset: u64) -> Self {
        Self { remaining: offset, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousIterator> StageIterator for OffsetStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
}

impl<Snapshot, PreviousIterator> LendingIterator for OffsetStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while self.remaining > 0 {
            if self.previous.next().is_none() {
                return None;
            }
            self.remaining -= 1;
        }
        self.previous.next()
    }
}

// Limit
pub struct LimitStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    limit_program: LimitProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> LimitStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(limit_program: LimitProgram, previous: PreviousStage) -> Self {
        Self { limit_program, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for LimitStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = LimitStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        self.previous.named_selected_outputs()
    }

    fn into_iterator(self, interrupt: ExecutionInterrupt) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let Self { limit_program, previous, .. } = self;
        let (previous_iterator, snapshot) = previous.into_iterator(interrupt)?;
        Ok((LimitStageIterator::new(previous_iterator, limit_program.limit), snapshot))
    }
}

pub struct LimitStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    remaining: u64,
    previous: PreviousIterator,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousIterator> LimitStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    fn new(previous: PreviousIterator, limit: u64) -> Self {
        Self { remaining: limit, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousIterator> StageIterator for LimitStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
}

impl<Snapshot, PreviousIterator> LendingIterator for LimitStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.previous.next()
        } else {
            None
        }
    }
}

// Filter
pub struct FilterStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    filter_program: FilterProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> FilterStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(filter_program: FilterProgram, previous: PreviousStage) -> Self {
        Self { filter_program, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for FilterStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = FilterStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        let prev = self.previous.named_selected_outputs();
        self.filter_program
            .retained_positions
            .iter()
            .map(|pos| (pos.clone(), prev.get(pos).unwrap().clone()))
            .collect::<HashMap<_, _>>()
    }

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let Self { filter_program, previous, .. } = self;
        let (previous_iterator, snapshot) = previous.into_iterator()?;
        Ok((FilterStageIterator::new(previous_iterator), snapshot))
    }
}

pub struct FilterStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    previous: PreviousIterator,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousIterator> FilterStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    fn new(previous: PreviousIterator) -> Self {
        Self { previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousIterator> StageIterator for FilterStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
}

impl<Snapshot, PreviousIterator> LendingIterator for FilterStageIterator<Snapshot, PreviousIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.previous.next()
    }
}
