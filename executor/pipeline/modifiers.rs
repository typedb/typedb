/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{
    VariablePosition,
    executable::modifiers::{
        DistinctExecutable, LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable,
    },
};
use ir::pipeline::modifier::SortVariable;
use lending_iterator::{LendingIterator, Peekable};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    ExecutionInterrupt,
    batch::Batch,
    pipeline::{
        PipelineExecutionError, StageIterator,
        stage::{ExecutionContext, StageAPI},
    },
    row::MaybeOwnedRow,
};

// Sort
pub struct SortStageExecutor<InputIterator> {
    executable: Arc<SortExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> SortStageExecutor<InputIterator> {
    pub fn new(executable: Arc<SortExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for SortStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = SortStageIterator;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { executable, .. } = self;
        // accumulate once, then we will operate in-place
        let batch = match input_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };
        let batch_len = batch.len();
        let profile = context.profile.profile_stage(|| String::from("Sort"), executable.executable_id);
        let pattern_profile = profile.create_or_get_pattern(|| String::from("Sort"));
        let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Sort execution"));
        let measurement = step_profile.start_measurement();
        let sorted_iterator =
            SortStageIterator::from_unsorted(batch, &executable, &context, step_profile.storage_counters());
        measurement.end(&step_profile, 1, batch_len as u64);
        Ok((sorted_iterator, context))
    }
}

pub struct SortStageIterator {
    unsorted: Batch,
    sorted_indices: Vec<usize>,
    next_index_index: usize,
}

impl SortStageIterator {
    fn from_unsorted(
        unsorted: Batch,
        sort_executable: &SortExecutable,
        context: &ExecutionContext<impl ReadableSnapshot>,
        storage_counters: StorageCounters,
    ) -> Self {
        let sort_by: Vec<(usize, bool)> = sort_executable
            .sort_on
            .iter()
            .map(|sort_variable| match sort_variable {
                SortVariable::Ascending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), true),
                SortVariable::Descending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), false),
            })
            .collect();
        let sorted_indices = unsorted.indices_sorted_by(context, &sort_by, storage_counters);
        Self { unsorted, sorted_indices, next_index_index: 0 }
    }
}

impl LendingIterator for SortStageIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

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
pub struct OffsetStageExecutor<InputIterator> {
    offset_executable: Arc<OffsetExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> OffsetStageExecutor<InputIterator> {
    pub fn new(offset_executable: Arc<OffsetExecutable>) -> Self {
        Self { offset_executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for OffsetStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = OffsetStageIterator<InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { offset_executable, .. } = self;
        Ok((OffsetStageIterator::new(input_iterator, offset_executable.offset), context))
    }
}

pub struct OffsetStageIterator<InputIterator> {
    remaining: u64,
    input: InputIterator,
}

impl<InputIterator> OffsetStageIterator<InputIterator> {
    fn new(input: InputIterator, offset: u64) -> Self {
        Self { remaining: offset, input }
    }
}

impl<InputIterator> StageIterator for OffsetStageIterator<InputIterator> where InputIterator: StageIterator {}

impl<InputIterator> LendingIterator for OffsetStageIterator<InputIterator>
where
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while self.remaining > 0 {
            match self.input.next() {
                None => return None,
                Some(Err(err)) => return Some(Err(err)),
                Some(Ok(_)) => (),
            }
            self.remaining -= 1;
        }
        self.input.next()
    }
}

// Limit
pub struct LimitStageExecutor<InputIterator> {
    limit_executable: Arc<LimitExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> LimitStageExecutor<InputIterator> {
    pub fn new(limit_executable: Arc<LimitExecutable>) -> Self {
        Self { limit_executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for LimitStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = LimitStageIterator<InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { limit_executable, .. } = self;
        Ok((LimitStageIterator::new(input_iterator, limit_executable.limit), context))
    }
}

pub struct LimitStageIterator<InputIterator> {
    remaining: u64,
    input: InputIterator,
}

impl<InputIterator> LimitStageIterator<InputIterator> {
    fn new(input: InputIterator, limit: u64) -> Self {
        Self { remaining: limit, input }
    }
}

impl<InputIterator> StageIterator for LimitStageIterator<InputIterator> where InputIterator: StageIterator {}

impl<InputIterator> LendingIterator for LimitStageIterator<InputIterator>
where
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.input.next()
        } else {
            None
        }
    }
}

// Select
pub struct SelectStageExecutor<InputIterator> {
    select_executable: Arc<SelectExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> SelectStageExecutor<InputIterator> {
    pub fn new(select_executable: Arc<SelectExecutable>) -> Self {
        Self { select_executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for SelectStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = SelectStageIterator<InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        Ok((SelectStageIterator::new(input_iterator, self.select_executable.retained_positions.clone()), context))
    }
}

pub struct SelectStageIterator<InputIterator> {
    input: InputIterator,
    retained_positions: HashSet<VariablePosition>,
}

impl<InputIterator> SelectStageIterator<InputIterator> {
    fn new(input: InputIterator, retained_positions: HashSet<VariablePosition>) -> Self {
        Self { input, retained_positions }
    }
}

impl<InputIterator> StageIterator for SelectStageIterator<InputIterator> where InputIterator: StageIterator {}

impl<InputIterator> LendingIterator for SelectStageIterator<InputIterator>
where
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.input.next().map(|res| {
            res.map(|row| {
                let (input, mult, provenance) = row.into_owned_parts();
                let mut output = Vec::with_capacity(input.len());
                for (i, val) in input.into_iter().enumerate() {
                    if self.retained_positions.contains(&VariablePosition::new(i as u32)) {
                        output.push(val);
                    } else {
                        output.push(VariableValue::None);
                    }
                }
                MaybeOwnedRow::new_owned(output, mult, provenance)
            })
        })
    }
}

// Require
pub struct RequireStageExecutor<InputIterator> {
    require_executable: Arc<RequireExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> RequireStageExecutor<InputIterator> {
    pub fn new(require_executable: Arc<RequireExecutable>) -> Self {
        Self { require_executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for RequireStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = RequireStageIterator<InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { require_executable, .. } = self;
        Ok((RequireStageIterator::new(input_iterator, require_executable), context))
    }
}

pub struct RequireStageIterator<InputIterator: LendingIterator> {
    require: Arc<RequireExecutable>,
    input: Peekable<InputIterator>,
}

impl<InputIterator: LendingIterator> RequireStageIterator<InputIterator> {
    fn new(input: InputIterator, require: Arc<RequireExecutable>) -> Self {
        Self { require, input: Peekable::new(input) }
    }
}

impl<InputIterator> StageIterator for RequireStageIterator<InputIterator> where InputIterator: StageIterator {}

impl<InputIterator> LendingIterator for RequireStageIterator<InputIterator>
where
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.input.peek() {
                None => return None,
                Some(Err(err)) => return Some(Err(err.clone())),
                Some(Ok(row)) => {
                    if self.require.required.iter().all(|&pos| !row.get(pos).is_none()) {
                        break;
                    }
                }
            }
        }
        self.input.next()
    }
}

// Distinct
pub struct DistinctStageExecutor<InputIterator> {
    executable: Arc<DistinctExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> DistinctStageExecutor<InputIterator> {
    pub fn new(executable: Arc<DistinctExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for DistinctStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = DistinctStageIterator<InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { .. } = self;
        let distinct_iterator = DistinctStageIterator::new(input_iterator);
        Ok((distinct_iterator, context))
    }
}

pub struct DistinctStageIterator<InputIterator> {
    seen: HashSet<MaybeOwnedRow<'static>>,
    input: InputIterator,
}

impl<InputIterator> DistinctStageIterator<InputIterator> {
    fn new(input_iterator: InputIterator) -> Self {
        Self { seen: HashSet::new(), input: input_iterator }
    }
}

impl<InputIterator> LendingIterator for DistinctStageIterator<InputIterator>
where
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.input.next() {
                None => return None,
                Some(Err(err)) => return Some(Err(err)),
                Some(Ok(row)) => {
                    if self.seen.insert(row.clone().into_owned()) {
                        return Some(Ok(row.clone().into_owned()));
                    }
                    // duplicate → continue loop instead of recursing
                }
            }
        }
    }
}

impl<InputIterator> StageIterator for DistinctStageIterator<InputIterator> where InputIterator: StageIterator {}
