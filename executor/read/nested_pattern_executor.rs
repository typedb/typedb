/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use answer::variable_value::VariableValue;
use compiler::{executable::match_::planner::match_executable::FunctionCallStep, VariablePosition};
use ir::pipeline::ParameterRegistry;

use crate::{
    batch::FixedBatch,
    read::{
        pattern_executor::{BranchIndex, PatternExecutor},
        step_executor::StepExecutors,
    },
    row::MaybeOwnedRow,
};

// TODO: Move Offset & Limit out of here, make the mapper stateless
pub(super) enum NestedPatternExecutor {
    Disjunction {
        branches: Vec<PatternExecutor>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    },
    Negation {
        inner: PatternExecutor,
    },
    InlinedFunction {
        inner: PatternExecutor,
        arg_mapping: Vec<VariablePosition>,
        return_mapping: Vec<VariablePosition>,
        output_width: u32,
        parameter_registry: Arc<ParameterRegistry>,
    },
    Offset {
        inner: PatternExecutor,
        offset: u64,
    },
    Limit {
        inner: PatternExecutor,
        limit: u64,
    },
}

impl Into<StepExecutors> for NestedPatternExecutor {
    fn into(self) -> StepExecutors {
        StepExecutors::Nested(self)
    }
}
impl NestedPatternExecutor {
    pub(crate) fn new_negation(inner: PatternExecutor) -> Self {
        Self::Negation { inner }
    }

    pub(crate) fn new_disjunction(
        branches: Vec<PatternExecutor>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self::Disjunction { branches, selected_variables, output_width }
    }

    pub(crate) fn new_inlined_function(
        inner: PatternExecutor,
        function_call: &FunctionCallStep,
        parameter_registry: Arc<ParameterRegistry>,
    ) -> Self {
        Self::InlinedFunction {
            inner,
            arg_mapping: function_call.arguments.clone(),
            return_mapping: function_call.assigned.clone(),
            output_width: function_call.output_width,
            parameter_registry,
        }
    }

    pub(crate) fn new_offset(inner: PatternExecutor, offset: u64) -> Self {
        Self::Offset { inner, offset }
    }

    pub(crate) fn new_limit(inner: PatternExecutor, limit: u64) -> Self {
        Self::Limit { inner, limit }
    }

    pub(crate) fn get_branch(&mut self, branch_index: BranchIndex) -> &mut PatternExecutor {
        debug_assert!(branch_index.0 == 0 || matches!(self, NestedPatternExecutor::Disjunction { .. }));
        match self {
            NestedPatternExecutor::Disjunction { branches, .. } => &mut branches[branch_index.0],
            NestedPatternExecutor::Negation { inner } => inner,
            NestedPatternExecutor::InlinedFunction { inner, .. } => inner,
            NestedPatternExecutor::Offset { inner, .. } => inner,
            NestedPatternExecutor::Limit { inner, .. } => inner,
        }
    }
}

// Bad name because I want to refactor later
pub(super) enum NestedPatternResultMapper {
    Select(SelectMapper),
    Negation(NegationMapper),
    InlinedFunction(InlinedFunctionMapper),
    Offset(OffsetMapper),
    Limit(LimitMapper),
}

impl NestedPatternResultMapper {
    pub(super) fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        match self {
            Self::Select(mapper) => mapper.map_input(input),
            Self::Negation(mapper) => mapper.map_input(input),
            Self::InlinedFunction(mapper) => mapper.map_input(input),
            Self::Offset(mapper) => mapper.map_input(input),
            Self::Limit(mapper) => mapper.map_input(input),
        }
    }

    pub(super) fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        match self {
            Self::Select(mapper) => mapper.map_output(subquery_result),
            Self::Negation(mapper) => mapper.map_output(subquery_result),
            Self::InlinedFunction(mapper) => mapper.map_output(subquery_result),
            Self::Offset(mapper) => mapper.map_output(subquery_result),
            Self::Limit(mapper) => mapper.map_output(subquery_result),
        }
    }
}

pub(super) trait NestedPatternResultMapperTrait {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static>;
    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl;
}

pub(super) enum NestedPatternControl {
    Retry(Option<FixedBatch>),
    Done(Option<FixedBatch>),
}

impl NestedPatternControl {
    pub(crate) fn into_parts(self) -> (bool, Option<FixedBatch>) {
        match self {
            NestedPatternControl::Retry(batch_opt) => (true, batch_opt),
            NestedPatternControl::Done(batch_opt) => (false, batch_opt),
        }
    }
}

pub(super) struct NegationMapper {
    input: MaybeOwnedRow<'static>,
}

pub(super) struct SelectMapper {
    selected_variables: Vec<VariablePosition>,
    output_width: u32,
}

impl SelectMapper {
    pub(super) fn new(selected_variables: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { selected_variables, output_width }
    }
}

pub(super) struct InlinedFunctionMapper {
    input: MaybeOwnedRow<'static>,
    arguments: Vec<VariablePosition>, // caller input -> callee input
    assigned: Vec<VariablePosition>,  // callee return -> caller output
    output_width: u32,                // This is for the caller.
}

impl NegationMapper {
    pub(crate) fn new(input: MaybeOwnedRow<'static>) -> Self {
        Self { input }
    }
}

impl NestedPatternResultMapperTrait for NegationMapper {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        match subquery_result {
            None => NestedPatternControl::Done(Some(FixedBatch::from(self.input.clone().into_owned()))),
            Some(batch) => {
                if batch.is_empty() {
                    NestedPatternControl::Retry(None)
                } else {
                    NestedPatternControl::Done(None)
                }
            }
        }
    }
}

impl NestedPatternResultMapperTrait for SelectMapper {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        if let Some(batch) = subquery_result {
            let mut uniform_batch = FixedBatch::new(self.output_width);
            for row in batch {
                uniform_batch.append(|mut output_row| {
                    output_row.copy_mapped(row, self.selected_variables.iter().map(|pos| (pos.clone(), pos.clone())));
                })
            }
            NestedPatternControl::Retry(Some(uniform_batch))
        } else {
            NestedPatternControl::Done(None)
        }
    }
}

impl InlinedFunctionMapper {
    pub(crate) fn new(
        input: MaybeOwnedRow<'static>,
        arguments: Vec<VariablePosition>,
        assigned: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self { input, arguments, assigned, output_width }
    }
}

impl NestedPatternResultMapperTrait for InlinedFunctionMapper {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        let args_owned: Vec<VariableValue<'static>> =
            self.arguments.iter().map(|arg_pos| input.get(arg_pos.clone()).clone().into_owned()).collect();
        MaybeOwnedRow::new_owned(args_owned, input.multiplicity())
    }

    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        match subquery_result {
            None => NestedPatternControl::Done(None),
            Some(returned_batch) => {
                let mut output_batch = FixedBatch::new(self.output_width);
                for return_index in 0..returned_batch.len() {
                    let returned_row = returned_batch.get_row(return_index);
                    output_batch.append(|mut output_row| {
                        output_row.copy_mapped(
                            self.input.as_reference(),
                            (0..self.input.len())
                                .map(|i| (VariablePosition::new(i as u32), VariablePosition::new(i as u32))),
                        );
                        output_row.copy_mapped(
                            returned_row.as_reference(),
                            self.assigned
                                .iter()
                                .enumerate()
                                .map(|(src, dst)| (VariablePosition::new(src as u32), dst.clone())),
                        );
                    });
                }
                NestedPatternControl::Retry(Some(output_batch))
            }
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

impl NestedPatternResultMapperTrait for OffsetMapper {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        self.current = 0;
        input.clone().into_owned()
    }

    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                NestedPatternControl::Retry(Some(input_batch))
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                NestedPatternControl::Retry(None)
            } else {
                let offset_in_batch = (self.required - self.current) as u32;
                let mut output_batch = FixedBatch::new(input_batch.width());
                for row_index in offset_in_batch..input_batch.len() {
                    output_batch.append(|mut output_row| {
                        let input_row = input_batch.get_row(row_index);
                        output_row.copy_from(input_row.row(), input_row.multiplicity())
                    });
                }
                self.current = self.required;
                NestedPatternControl::Retry(Some(output_batch))
            }
        } else {
            NestedPatternControl::Done(None)
        }
    }
}

pub(super) struct LimitMapper {
    required: u64,
    current: u64,
}

impl LimitMapper {
    pub(crate) fn new(limit: u64) -> Self {
        Self { required: limit, current: limit }
    }
}

impl NestedPatternResultMapperTrait for LimitMapper {
    fn map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        self.current = 0;
        input.clone().into_owned()
    }

    fn map_output(&mut self, subquery_result: Option<FixedBatch>) -> NestedPatternControl {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                NestedPatternControl::Done(None)
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                NestedPatternControl::Retry(Some(input_batch))
            } else {
                let mut output_batch = FixedBatch::new(input_batch.width());
                let mut i = 0;
                while self.current < self.required {
                    output_batch.append(|mut output_row| {
                        let input_row = input_batch.get_row(i);
                        output_row.copy_from(input_row.row(), input_row.multiplicity())
                    });
                    i += 1;
                    self.current += 1;
                }
                debug_assert!(self.current == self.required);
                NestedPatternControl::Done(Some(output_batch))
            }
        } else {
            NestedPatternControl::Done(None)
        }
    }
}
