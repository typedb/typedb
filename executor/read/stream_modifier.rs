/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use encoding::value::value::Value;

use crate::{
    batch::FixedBatch,
    read::{pattern_executor::PatternExecutor, step_executor::StepExecutors},
    row::MaybeOwnedRow,
    Provenance,
};

#[derive(Debug)]
pub(crate) enum StreamModifierExecutor {
    Select { inner: PatternExecutor, removed_positions: Vec<VariablePosition> },
    Offset { inner: PatternExecutor, offset: u64 },
    Limit { inner: PatternExecutor, limit: u64 },
    Distinct { inner: PatternExecutor },
    Last { inner: PatternExecutor },
    Check { inner: PatternExecutor },
}

impl From<StreamModifierExecutor> for StepExecutors {
    fn from(val: StreamModifierExecutor) -> Self {
        StepExecutors::StreamModifier(val)
    }
}

impl StreamModifierExecutor {
    pub(crate) fn new_select(inner: PatternExecutor, removed_positions: Vec<VariablePosition>) -> Self {
        Self::Select { inner, removed_positions }
    }

    pub(crate) fn new_offset(inner: PatternExecutor, offset: u64) -> Self {
        Self::Offset { inner, offset }
    }

    pub(crate) fn new_limit(inner: PatternExecutor, limit: u64) -> Self {
        Self::Limit { inner, limit }
    }

    pub(crate) fn new_distinct(inner: PatternExecutor) -> Self {
        Self::Distinct { inner }
    }

    pub(crate) fn new_first(inner: PatternExecutor) -> Self {
        const FIRST_LIMIT: u64 = 1;
        Self::new_limit(inner, FIRST_LIMIT)
    }

    pub(crate) fn new_last(inner: PatternExecutor) -> Self {
        Self::Last { inner }
    }

    pub(crate) fn new_check(inner: PatternExecutor) -> Self {
        Self::Check { inner }
    }

    pub(crate) fn output_width(&self) -> u32 {
        match self {
            StreamModifierExecutor::Select { inner, .. }
            | StreamModifierExecutor::Offset { inner, .. }
            | StreamModifierExecutor::Limit { inner, .. }
            | StreamModifierExecutor::Distinct { inner }
            | StreamModifierExecutor::Last { inner } => inner.output_width(),
            StreamModifierExecutor::Check { .. } => 1u32,
        }
    }

    pub(crate) fn inner(&mut self) -> &mut PatternExecutor {
        match self {
            Self::Select { inner, .. } => inner,
            Self::Offset { inner, .. } => inner,
            Self::Limit { inner, .. } => inner,
            Self::Distinct { inner, .. } => inner,
            Self::Last { inner, .. } => inner,
            Self::Check { inner, .. } => inner,
        }
    }

    pub(crate) fn create_mapper(&self) -> StreamModifierResultMapper {
        match self {
            Self::Select { removed_positions, .. } => {
                StreamModifierResultMapper::Select(SelectMapper::new(removed_positions.clone()))
            }
            Self::Offset { offset, .. } => StreamModifierResultMapper::Offset(OffsetMapper::new(*offset)),
            Self::Limit { limit, .. } => StreamModifierResultMapper::Limit(LimitMapper::new(*limit)),
            Self::Distinct { .. } => StreamModifierResultMapper::Distinct(DistinctMapper::new()),
            Self::Last { .. } => StreamModifierResultMapper::Last(LastMapper::new()),
            Self::Check { .. } => StreamModifierResultMapper::Check(CheckMapper::new()),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.inner().reset()
    }
}

#[derive(Debug)]
pub(super) enum StreamModifierResultMapper {
    Select(SelectMapper),
    Offset(OffsetMapper),
    Limit(LimitMapper),
    Distinct(DistinctMapper),
    Last(LastMapper),
    Check(CheckMapper),
}

impl StreamModifierResultMapper {
    pub(super) fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        match self {
            Self::Select(inner) => inner.map_output(subquery_result),
            Self::Offset(inner) => inner.map_output(subquery_result),
            Self::Limit(inner) => inner.map_output(subquery_result),
            Self::Distinct(inner) => inner.map_output(subquery_result),
            Self::Last(inner) => inner.map_output(subquery_result),
            Self::Check(inner) => inner.map_output(subquery_result),
        }
    }
}

trait StreamModifierResultMapperTrait {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch>;
}

#[derive(Debug)]
pub(super) struct SelectMapper {
    removed_positions: Vec<VariablePosition>,
}

impl SelectMapper {
    pub(crate) fn new(removed_positions: Vec<VariablePosition>) -> Self {
        Self { removed_positions }
    }
}

impl StreamModifierResultMapperTrait for SelectMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        if let Some(mut input_batch) = subquery_result {
            for i in 0..input_batch.len() {
                let mut row = input_batch.get_row_mut(i);
                for pos in self.removed_positions.iter() {
                    row.unset(*pos);
                }
            }
            Some(input_batch)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(super) struct OffsetMapper {
    required: u64,
    current: u64,
}

impl OffsetMapper {
    pub(crate) fn new(offset: u64) -> Self {
        Self { required: offset, current: 0 }
    }
}

impl StreamModifierResultMapperTrait for OffsetMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                Some(input_batch)
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                Some(FixedBatch::EMPTY) // Retry this instruction without returning any rows
            } else {
                let offset_in_batch = (self.required - self.current) as u32;
                let mut output_batch = FixedBatch::new(input_batch.width());
                for row_index in offset_in_batch..input_batch.len() {
                    output_batch.append(|mut output_row| output_row.copy_from_row(input_batch.get_row(row_index)));
                }
                self.current = self.required;
                Some(output_batch)
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(super) struct LimitMapper {
    required: u64,
    current: u64,
}

impl LimitMapper {
    pub(crate) fn new(limit: u64) -> Self {
        Self { required: limit, current: 0 }
    }
}

impl StreamModifierResultMapperTrait for LimitMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                None
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                Some(input_batch)
            } else {
                let mut output_batch = FixedBatch::new(input_batch.width());
                let mut i = 0;
                while self.current < self.required {
                    output_batch.append(|mut output_row| {
                        output_row.copy_from_row(input_batch.get_row(i));
                    });
                    i += 1;
                    self.current += 1;
                }
                debug_assert!(self.current == self.required);
                Some(output_batch)
            }
        } else {
            None
        }
    }
}

// Distinct
#[derive(Debug)]
pub(super) struct DistinctMapper {
    collector: HashSet<MaybeOwnedRow<'static>>,
}

impl DistinctMapper {
    pub(crate) fn new() -> Self {
        Self { collector: HashSet::new() }
    }
}

impl StreamModifierResultMapperTrait for DistinctMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        let mut input_batch = subquery_result?;
        for i in 0..input_batch.len() {
            // Don't let multiplicity & provenance come into the picture:
            let without_metadata =
                MaybeOwnedRow::new_borrowed(input_batch.get_row(i).row(), &1, &Provenance::INITIAL).into_owned();
            if !self.collector.insert(without_metadata) {
                input_batch.get_row_mut(i).set_multiplicity(0);
            } else {
                input_batch.get_row_mut(i).set_multiplicity(1);
            }
        }
        Some(input_batch)
    }
}

#[derive(Debug)]
pub(super) struct LastMapper {
    last_row: Option<MaybeOwnedRow<'static>>,
}

impl LastMapper {
    pub(crate) fn new() -> Self {
        Self { last_row: None }
    }
}

impl StreamModifierResultMapperTrait for LastMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        if let Some(input_batch) = subquery_result {
            self.last_row = (!input_batch.is_empty()).then(|| input_batch.get_row(input_batch.len() - 1).into_owned());
            Some(FixedBatch::EMPTY) // Retry this instruction without returning any rows
        } else {
            self.last_row.take().map(FixedBatch::from)
        }
    }
}

#[derive(Debug)]
pub(super) struct CheckMapper {
    returned: Option<bool>,
}

impl CheckMapper {
    pub(crate) fn new() -> Self {
        Self { returned: None }
    }
}

impl StreamModifierResultMapperTrait for CheckMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        if self.returned.is_some() {
            return None;
        } else {
            match subquery_result {
                None => {
                    self.returned = Some(false);
                    let false_row = MaybeOwnedRow::new_owned(
                        vec![VariableValue::Value(Value::Boolean(false))],
                        1,
                        Provenance::INITIAL,
                    );
                    Some(FixedBatch::from(false_row))
                }
                Some(batch) => {
                    if batch.is_empty() {
                        Some(FixedBatch::EMPTY)
                    } else {
                        self.returned = Some(true);
                        let true_row = MaybeOwnedRow::new_owned(
                            vec![VariableValue::Value(Value::Boolean(true))],
                            1,
                            batch.get_row(0).provenance(),
                        );
                        Some(FixedBatch::from(true_row))
                    }
                }
            }
        }
    }
}
