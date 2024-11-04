/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use crate::{
    batch::FixedBatch,
    read::{pattern_executor::PatternExecutor, step_executor::StepExecutors},
    row::MaybeOwnedRow,
};

pub(super) enum StreamModifierExecutor {
    Offset { inner: PatternExecutor, offset: u64 },
    Limit { inner: PatternExecutor, limit: u64 },
    Distinct { inner: PatternExecutor, output_width: u32 },
}

impl Into<StepExecutors> for StreamModifierExecutor {
    fn into(self) -> StepExecutors {
        StepExecutors::StreamModifier(self)
    }
}

impl StreamModifierExecutor {
    pub(crate) fn new_offset(inner: PatternExecutor, offset: u64) -> Self {
        Self::Offset { inner, offset }
    }

    pub(crate) fn new_limit(inner: PatternExecutor, limit: u64) -> Self {
        Self::Limit { inner, limit }
    }

    pub(crate) fn new_distinct(inner: PatternExecutor, output_width: u32) -> Self {
        Self::Distinct { inner, output_width }
    }

    pub(crate) fn get_inner(&mut self) -> &mut PatternExecutor {
        match self {
            Self::Offset { inner, .. } => inner,
            Self::Limit { inner, .. } => inner,
            Self::Distinct { inner, .. } => inner,
        }
    }
}

pub(super) enum StreamModifierResultMapper {
    Offset(OffsetMapper),
    Limit(LimitMapper),
    Distinct(DistinctMapper),
}

impl StreamModifierResultMapper {
    pub(super) fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> StreamModifierControl {
        match self {
            Self::Offset(mapper) => mapper.map_output(subquery_result),
            Self::Limit(mapper) => mapper.map_output(subquery_result),
            Self::Distinct(mapper) => mapper.map_output(subquery_result),
        }
    }
}

pub(super) trait StreamModifierResultMapperTrait {
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> StreamModifierControl;
}

pub(super) enum StreamModifierControl {
    Retry(Option<FixedBatch>),
    Done(Option<FixedBatch>),
}

impl StreamModifierControl {
    pub(crate) fn into_parts(self) -> (bool, Option<FixedBatch>) {
        match self {
            StreamModifierControl::Retry(batch_opt) => (true, batch_opt),
            StreamModifierControl::Done(batch_opt) => (false, batch_opt),
        }
    }
}

// TODO: OffsetController & LimitController should be really easy.
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
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> StreamModifierControl {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                StreamModifierControl::Retry(Some(input_batch))
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                StreamModifierControl::Retry(None)
            } else {
                let offset_in_batch = (self.required - self.current) as u32;
                let mut output_batch = FixedBatch::new(input_batch.width());
                for row_index in offset_in_batch..input_batch.len() {
                    output_batch.append(|mut output_row| output_row.copy_from_row(input_batch.get_row(row_index)));
                }
                self.current = self.required;
                StreamModifierControl::Retry(Some(output_batch))
            }
        } else {
            StreamModifierControl::Done(None)
        }
    }
}

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
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> StreamModifierControl {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                StreamModifierControl::Done(None)
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                StreamModifierControl::Retry(Some(input_batch))
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
                StreamModifierControl::Done(Some(output_batch))
            }
        } else {
            StreamModifierControl::Done(None)
        }
    }
}

// Distinct
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
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> StreamModifierControl {
        if let Some(input_batch) = subquery_result {
            let mut output_batch = FixedBatch::new(self.output_width);
            for row in input_batch {
                if self.collector.insert(row.clone().into_owned()) {
                    output_batch.append(|mut output_row| output_row.copy_from_row(row));
                }
            }
            StreamModifierControl::Retry(Some(output_batch))
        } else {
            StreamModifierControl::Done(None)
        }
    }
}
