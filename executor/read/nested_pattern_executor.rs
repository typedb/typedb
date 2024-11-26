/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{executable::match_::planner::match_executable::FunctionCallStep, VariablePosition};
use ir::pipeline::ParameterRegistry;

use crate::{
    batch::FixedBatch,
    read::{pattern_executor::PatternExecutor, step_executor::StepExecutors},
    row::MaybeOwnedRow,
};

pub(super) struct Disjunction {
    pub branches: Vec<PatternExecutor>,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl Disjunction {
    pub(crate) fn map_output(&self, unmapped: FixedBatch) -> FixedBatch {
        let mut uniform_batch = FixedBatch::new(self.output_width);
        unmapped.into_iter().for_each(|row| {
            uniform_batch.append(|mut output_row| {
                output_row.copy_mapped(row, self.selected_variables.iter().map(|&pos| (pos, pos)));
            })
        });
        uniform_batch
    }
}

pub(super) struct Negation {
    pub inner: PatternExecutor,
}

pub(super) struct InlinedFunction {
    pub inner: PatternExecutor,
    pub arg_mapping: Vec<VariablePosition>,
    pub return_mapping: Vec<VariablePosition>,
    pub output_width: u32,
    pub parameter_registry: Arc<ParameterRegistry>,
}

impl InlinedFunction {
    pub(crate) fn map_output(&self, input: MaybeOwnedRow<'_>, batch: FixedBatch) -> FixedBatch {
        let mut output_batch = FixedBatch::new(self.output_width);
        for return_index in 0..batch.len() {
            let returned_row = batch.get_row(return_index);
            output_batch.append(|mut output_row| {
                output_row.copy_from_row(input.as_reference());
                output_row.copy_mapped(
                    returned_row.as_reference(),
                    self.return_mapping.iter().enumerate().map(|(src, &dst)| (VariablePosition::new(src as u32), dst)),
                );
            });
        }
        output_batch
    }
}

pub(crate) enum NestedPatternExecutor {
    Disjunction(Disjunction),
    Negation(Negation),
    InlinedFunction(InlinedFunction),
}

impl From<NestedPatternExecutor> for StepExecutors {
    fn from(val: NestedPatternExecutor) -> Self {
        StepExecutors::Nested(val)
    }
}
impl NestedPatternExecutor {
    pub(crate) fn new_negation(inner: PatternExecutor) -> Self {
        Self::Negation(Negation { inner })
    }

    pub(crate) fn new_disjunction(
        branches: Vec<PatternExecutor>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self::Disjunction(Disjunction { branches, selected_variables, output_width })
    }

    pub(crate) fn new_inlined_function(
        inner: PatternExecutor,
        function_call: &FunctionCallStep,
        parameter_registry: Arc<ParameterRegistry>,
    ) -> Self {
        Self::InlinedFunction(InlinedFunction {
            inner,
            arg_mapping: function_call.arguments.clone(),
            return_mapping: function_call.assigned.clone(),
            output_width: function_call.output_width,
            parameter_registry,
        })
    }
}
