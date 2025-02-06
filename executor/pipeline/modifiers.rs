/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{borrow::Cow, cmp::Ordering, sync::Arc};
use std::collections::HashSet;
use std::hash::{Hash, Hasher, DefaultHasher};
use answer::{variable_value::VariableValue, Thing};
use compiler::executable::modifiers::{
    LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable, DistinctExecutable
};
use compiler::VariablePosition;
use encoding::value::value::Value;
use error::unimplemented_feature;
use ir::pipeline::modifier::SortVariable;
use lending_iterator::{LendingIterator, Peekable};
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
    executable: Arc<SortExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> SortStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<SortExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { previous, executable, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        // accumulate once, then we will operate in-place
        let batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };
        let batch_len = batch.len();
        let profile = context.profile.profile_stage(|| String::from("Sort"), executable.executable_id);
        let step_profile = profile.extend_or_get(0, || String::from("Sort execution"));
        let measurement = step_profile.start_measurement();
        let sorted_iterator = SortStageIterator::from_unsorted(batch, &executable, &context);
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
    ) -> Self {
        let mut indices: Vec<usize> = (0..unsorted.len()).collect();
        let sort_by: Vec<(usize, bool)> = sort_executable
            .sort_on
            .iter()
            .map(|sort_variable| match sort_variable {
                SortVariable::Ascending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), true),
                SortVariable::Descending(v) => (sort_executable.output_row_mapping.get(v).unwrap().as_usize(), false),
            })
            .collect();
        indices.sort_by(|x, y| {
            let x_row_as_row = unsorted.get_row(*x);
            let y_row_as_row = unsorted.get_row(*y);
            let x_row = x_row_as_row.row();
            let y_row = y_row_as_row.row();
            for &(idx, asc) in &sort_by {
                let ord = Self::get_value(&x_row[idx], context)
                    .partial_cmp(&Self::get_value(&y_row[idx], context))
                    .expect("Sort on variable with uncomparable values should have been caught at query-compile time");
                if ord != Ordering::Equal {
                    if asc {
                        return ord;
                    } else {
                        return ord.reverse();
                    }
                };
            }
            Ordering::Equal
        });
        Self { unsorted, sorted_indices: indices, next_index_index: 0 }
    }

    fn get_value<'a, T: ReadableSnapshot>(
        entry: &'a VariableValue<'a>,
        context: &'a ExecutionContext<T>,
    ) -> Option<Cow<'a, Value<'a>>> {
        let snapshot: &T = &context.snapshot;
        match entry {
            VariableValue::Value(value) => Some(Cow::Borrowed(value)),
            VariableValue::Thing(Thing::Attribute(attribute)) => {
                Some(Cow::Owned(attribute.get_value(snapshot, &context.thing_manager).unwrap()))
            }
            VariableValue::Empty => { None }
            VariableValue::Type(_) | VariableValue::Thing(_) => {
                unreachable!("Should have been caught earlier")
            }

            | VariableValue::ThingList(_) => unimplemented_feature!(Lists),
            | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        }
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
pub struct OffsetStageExecutor<PreviousStage> {
    offset_executable: Arc<OffsetExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> OffsetStageExecutor<PreviousStage> {
    pub fn new(offset_executable: Arc<OffsetExecutable>, previous: PreviousStage) -> Self {
        Self { offset_executable, previous }
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { offset_executable, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((OffsetStageIterator::new(previous_iterator, offset_executable.offset), context))
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
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while self.remaining > 0 {
            match self.previous.next() {
                None => return None,
                Some(Err(err)) => return Some(Err(err)),
                Some(Ok(_)) => (),
            }
            self.remaining -= 1;
        }
        self.previous.next()
    }
}

// Limit
pub struct LimitStageExecutor<PreviousStage> {
    limit_executable: Arc<LimitExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> LimitStageExecutor<PreviousStage> {
    pub fn new(limit_executable: Arc<LimitExecutable>, previous: PreviousStage) -> Self {
        Self { limit_executable, previous }
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { limit_executable, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((LimitStageIterator::new(previous_iterator, limit_executable.limit), context))
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
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

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
    select_executable: Arc<SelectExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> SelectStageExecutor<PreviousStage> {
    pub fn new(select_executable: Arc<SelectExecutable>, previous: PreviousStage) -> Self {
        Self { select_executable, previous }
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
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
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.previous.next()
    }
}

// Require
pub struct RequireStageExecutor<PreviousStage> {
    require_executable: Arc<RequireExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> RequireStageExecutor<PreviousStage> {
    pub fn new(require_executable: Arc<RequireExecutable>, previous: PreviousStage) -> Self {
        Self { require_executable, previous }
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { require_executable, previous, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        Ok((RequireStageIterator::new(previous_iterator, require_executable), context))
    }
}

pub struct RequireStageIterator<PreviousIterator: LendingIterator> {
    require: Arc<RequireExecutable>,
    previous: Peekable<PreviousIterator>,
}

impl<PreviousIterator: LendingIterator> RequireStageIterator<PreviousIterator> {
    fn new(previous: PreviousIterator, require: Arc<RequireExecutable>) -> Self {
        Self { require, previous: Peekable::new(previous) }
    }
}

impl<PreviousIterator> StageIterator for RequireStageIterator<PreviousIterator> where PreviousIterator: StageIterator {}

impl<PreviousIterator> LendingIterator for RequireStageIterator<PreviousIterator>
where
    PreviousIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        loop {
            match self.previous.peek() {
                None => return None,
                Some(Err(err)) => return Some(Err(err.clone())),
                Some(Ok(row)) => {
                    if self.require.required.iter().all(|&pos| !row.get(pos).is_empty()) {
                        break;
                    }
                }
            }
        }
        self.previous.next()
    }
}

// Distinct
pub struct DistinctStageExecutor<PreviousStage> {
    executable: Arc<DistinctExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> DistinctStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<DistinctExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for DistinctStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = DistinctStageIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { previous, executable, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        // accumulate once, then we will operate in-place
        let batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };
        let batch_len = batch.len();
        let profile = context.profile.profile_stage(|| String::from("Distinct"), executable.executable_id);
        let step_profile = profile.extend_or_get(0, || String::from("Distinct execution"));
        let measurement = step_profile.start_measurement();
        let distinct_iterator = DistinctStageIterator::from_batch_with_duplicates(batch, &executable, &context, executable.output_row_mapping.values().collect());
        measurement.end(&step_profile, 1, batch_len as u64);
        Ok((distinct_iterator, context))
    }
}

pub struct DistinctStageIterator {
    batch_with_duplicates: Batch,
    next_row_index: usize,
    // Record contiguous index blocks of duplicates:
    duplicate_block_start_indices: Vec<usize>, // invariant: nui.len() <= fdi.len() <= nui.len() + 1
    unique_block_restart_indices: Vec<usize>,
    next_duplicate_index_index: usize,
}

impl DistinctStageIterator {
    fn from_batch_with_duplicates(
        batch_with_duplicates: Batch,
        sort_executable: &DistinctExecutable,
        context: &ExecutionContext<impl ReadableSnapshot>,
        variable_positions: Vec<VariablePosition>,
    ) -> Self {
        let mut indices: Vec<usize> = (0..batch_with_duplicates.len()).collect();
        let mut duplicate_block_start_indices: Vec<usize> = vec![];
        let mut unique_block_restart_indices: Vec<usize> = vec![];
        let mut previously_seen_hashes: HashSet<u64> = HashSet::new();
        let mut looking_for_duplicate = true;

        for &row_index in indices.iter() {
            let row = batch_with_duplicates.get_row(row_index);
            let mut hasher = DefaultHasher::new();
            for &pos in &variable_positions {
                row.get(pos).hash(&mut hasher);
            }
            let hash = hasher.finish();
            if !previously_seen_hashes.contains(&hash) {
                previously_seen_hashes.insert(hash);
                if looking_for_duplicate == false {
                    looking_for_duplicate = true;
                    unique_block_restart_indices.push(row_index)
                }
            } else {
                if looking_for_duplicate == true {
                    looking_for_duplicate = false;
                    duplicate_block_start_indices.push(row_index)
                }
            }
        }

        Self { batch_with_duplicates, next_row_index: 0, duplicate_block_start_indices, unique_block_restart_indices, next_duplicate_index_index: 0 }
    }
}

impl LendingIterator for DistinctStageIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.next_row_index < self.batch_with_duplicates.len() {
            if self.next_duplicate_index_index >= self.duplicate_block_start_indices.len() || self.next_row_index < self.duplicate_block_start_indices[self.next_duplicate_index_index] {
                // Case 1: next row is not a duplicate
                let next_row = self.batch_with_duplicates.get_row(self.next_row_index);
                self.next_row_index += 1;
                Some(Ok(next_row))
            } else {
                // Case 2: next row *is* a duplicate
                if let Some(next_index) = self.unique_block_restart_indices.get(self.next_duplicate_index_index) {
                    let next_row = self.batch_with_duplicates.get_row(self.next_row_index);
                    self.next_row_index = *next_index;
                    Some(Ok(next_row))
                } else {
                    // There are no more non-duplicates
                    None
                }
            }
        } else {
            None
        }
    }
}

impl StageIterator for DistinctStageIterator {}
