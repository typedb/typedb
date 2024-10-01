/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{cmp::Ordering, collections::HashSet};

use answer::variable_value::VariableValue;
use compiler::{
    modifiers::{LimitProgram, OffsetProgram, RequireProgram, SelectProgram, SortProgram},
    VariablePosition,
};
use ir::program::modifier::SortVariable;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

// Sort
pub struct SortStageExecutor<PreviousStage> {
    program: SortProgram,
    previous: PreviousStage,
}

impl<PreviousStage> SortStageExecutor<PreviousStage> {
    pub fn new(program: SortProgram, previous: PreviousStage) -> Self {
        Self { program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for SortStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = SortStageIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { previous, program, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        // accumulate once, then we will operate in-place
        let batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };
        Ok((SortStageIterator::from_unsorted(batch, program), context))
    }
}

pub struct SortStageIterator {
    unsorted: Batch,
    sorted_indices: Vec<usize>,
    next_index_index: usize,
}

impl SortStageIterator {
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
            let x_row = x_row_as_row.row();
            let y_row = y_row_as_row.row();
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
        Self { unsorted, sorted_indices: indices, next_index_index: 0 }
    }
}

impl LendingIterator for SortStageIterator {
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

impl StageIterator for SortStageIterator {}

// Offset
pub struct OffsetStageExecutor<PreviousStage> {
    offset_program: OffsetProgram,
    previous: PreviousStage,
}

impl<PreviousStage> OffsetStageExecutor<PreviousStage> {
    pub fn new(offset_program: OffsetProgram, previous: PreviousStage) -> Self {
        Self { offset_program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for OffsetStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = OffsetStageIterator<PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { offset_program, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((OffsetStageIterator::new(previous_iterator, offset_program.offset), context))
    }
}

pub struct OffsetStageIterator<PreviousIterator> {
    remaining: u64,
    previous: PreviousIterator,
}

impl<PreviousIterator> OffsetStageIterator<PreviousIterator> {
    fn new(previous: PreviousIterator, offset: u64) -> Self {
        Self { remaining: offset, previous }
    }
}

impl<PreviousIterator> StageIterator for OffsetStageIterator<PreviousIterator> where PreviousIterator: StageIterator {}

impl<PreviousIterator> LendingIterator for OffsetStageIterator<PreviousIterator>
where
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
pub struct LimitStageExecutor<PreviousStage> {
    limit_program: LimitProgram,
    previous: PreviousStage,
}

impl<PreviousStage> LimitStageExecutor<PreviousStage> {
    pub fn new(limit_program: LimitProgram, previous: PreviousStage) -> Self {
        Self { limit_program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for LimitStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = LimitStageIterator<PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { limit_program, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((LimitStageIterator::new(previous_iterator, limit_program.limit), context))
    }
}

pub struct LimitStageIterator<PreviousIterator> {
    remaining: u64,
    previous: PreviousIterator,
}

impl<PreviousIterator> LimitStageIterator<PreviousIterator> {
    fn new(previous: PreviousIterator, limit: u64) -> Self {
        Self { remaining: limit, previous }
    }
}

impl<PreviousIterator> StageIterator for LimitStageIterator<PreviousIterator> where PreviousIterator: StageIterator {}

impl<PreviousIterator> LendingIterator for LimitStageIterator<PreviousIterator>
where
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

// Select
pub struct SelectStageExecutor<PreviousStage> {
    select_program: SelectProgram,
    previous: PreviousStage,
}

impl<PreviousStage> SelectStageExecutor<PreviousStage> {
    pub fn new(select_program: SelectProgram, previous: PreviousStage) -> Self {
        Self { select_program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for SelectStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = SelectStageIterator<PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((SelectStageIterator::new(previous_iterator), context))
    }
}

pub struct SelectStageIterator<PreviousIterator> {
    previous: PreviousIterator,
}

impl<PreviousIterator> SelectStageIterator<PreviousIterator> {
    fn new(previous: PreviousIterator) -> Self {
        Self { previous }
    }
}

impl<PreviousIterator> StageIterator for SelectStageIterator<PreviousIterator> where PreviousIterator: StageIterator {}

impl<PreviousIterator> LendingIterator for SelectStageIterator<PreviousIterator>
where
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.previous.next()
    }
}

// Require
pub struct RequireStageExecutor<PreviousStage> {
    require_program: RequireProgram,
    previous: PreviousStage,
}

impl<PreviousStage> RequireStageExecutor<PreviousStage> {
    pub fn new(require_program: RequireProgram, previous: PreviousStage) -> Self {
        Self { require_program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for RequireStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = RequireStageIterator<PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { require_program, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((RequireStageIterator::new(previous_iterator, require_program.required), context))
    }
}

pub struct RequireStageIterator<PreviousIterator> {
    required: HashSet<VariablePosition>,
    previous: PreviousIterator,
}

impl<PreviousIterator> RequireStageIterator<PreviousIterator> {
    fn new(previous: PreviousIterator, required: HashSet<VariablePosition>) -> Self {
        Self { required: required, previous }
    }
}

impl<PreviousIterator> StageIterator for RequireStageIterator<PreviousIterator> where PreviousIterator: StageIterator {}

impl<PreviousIterator> LendingIterator for RequireStageIterator<PreviousIterator>
where
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            let next = self.previous.next();
            match next {
                None => {
                    return None;
                }
                Some(Err(err)) => {
                    return Some(Err(err));
                }
                Some(Ok(row)) => {
                    for pos in self.required.iter() {
                        if matches!(row.get(*pos), &VariableValue::Empty) {
                            continue;
                        }
                    }
                    return Some(Ok(row));
                }
            }
        }
    }
}
