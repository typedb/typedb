/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use compiler::VariablePosition;
use utils::{enum_dispatch_method, enum_dispatch};

use crate::{
    batch::FixedBatch,
    read::{pattern_executor::PatternExecutor, step_executor::StepExecutors},
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) enum StreamModifierExecutor {
    Select { inner: PatternExecutor, removed_positions: Vec<VariablePosition> },
    Offset { inner: PatternExecutor, offset: u64 },
    Limit { inner: PatternExecutor, limit: u64 },
    Distinct { inner: PatternExecutor, output_width: u32 },
    Last { inner: PatternExecutor },
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

    pub(crate) fn new_distinct(inner: PatternExecutor, output_width: u32) -> Self {
        Self::Distinct { inner, output_width }
    }

    pub(crate) fn new_first(inner: PatternExecutor) -> Self {
        const FIRST_LIMIT: u64 = 1;
        Self::new_limit(inner, FIRST_LIMIT)
    }

    pub(crate) fn new_last(inner: PatternExecutor) -> Self {
        Self::Last { inner }
    }

    pub(crate) fn inner(&mut self) -> &mut PatternExecutor {
        match self {
            Self::Select { inner, .. } => inner,
            Self::Offset { inner, .. } => inner,
            Self::Limit { inner, .. } => inner,
            Self::Distinct { inner, .. } => inner,
            Self::Last { inner, .. } => inner,
        }
    }

    pub(crate) fn create_mapper(&self) -> StreamModifierResultMapper {
        match self {
            StreamModifierExecutor::Select { removed_positions, .. } => {
                StreamModifierResultMapper::Select(SelectMapper::new(removed_positions.clone()))
            }
            StreamModifierExecutor::Offset { offset, .. } => {
                StreamModifierResultMapper::Offset(OffsetMapper::new(*offset))
            }
            StreamModifierExecutor::Limit { limit, .. } => StreamModifierResultMapper::Limit(LimitMapper::new(*limit)),
            StreamModifierExecutor::Distinct { output_width, .. } => {
                StreamModifierResultMapper::Distinct(DistinctMapper::new(*output_width))
            }
            StreamModifierExecutor::Last { .. } => StreamModifierResultMapper::Last(LastMapper::new()),
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
}

impl StreamModifierResultMapper {
    enum_dispatch_method! {
        pub(super) fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch>  [
            Select Offset Limit Distinct Last
        ]
    }
}

trait StreamModifierResultMapperTrait {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch>;
    fn reset(&mut self);
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

    fn reset(&mut self) { }
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
                Some(FixedBatch::EMPTY)
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

    fn reset(&mut self) {
        self.current = 0
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

    fn reset(&mut self) {
        self.current = 0;
    }
}

// Distinct
#[derive(Debug)]
pub(super) struct DistinctMapper {
    collector: HashSet<MaybeOwnedRow<'static>>,
    output_width: u32,
}

impl DistinctMapper {
    pub(crate) fn new(output_width: u32) -> Self {
        Self { collector: HashSet::new(), output_width }
    }
}

impl StreamModifierResultMapperTrait for DistinctMapper {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> Option<FixedBatch> {
        let mut input_batch = subquery_result?;
        if input_batch.is_empty() { // Wait why?
            return None
        };

        for i in 0..input_batch.len() {
            if !self.collector.insert(input_batch.get_row(i).into_owned()) {
                let mut row = input_batch.get_row_mut(i);
                row.set_multiplicity(0);
            }
        }
        Some(input_batch)
    }

    fn reset(&mut self) {
        self.collector = HashSet::new() // reallocates to small
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
            self.last_row = Some(input_batch.get_row(input_batch.len() - 1).into_owned());
            Some(FixedBatch::EMPTY)
        } else {
            self.last_row.take().map(FixedBatch::from)
        }
    }

    fn reset(&mut self) {
        self.last_row = None
    }
}
