/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use answer::variable_value::VariableValue;
use compiler::{executable::match_::planner::conjunction_executable::FunctionCallStep, VariablePosition};
use ir::{pattern::BranchID, pipeline::ParameterRegistry};

use crate::{
    batch::FixedBatch,
    read::{pattern_executor::PatternExecutor, step_executor::StepExecutors, BranchIndex},
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub struct DisjunctionExecutor {
    pub branches: Vec<PatternExecutor>,
    pub branch_ids: Vec<BranchID>,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl DisjunctionExecutor {
    pub(crate) fn new(
        branch_ids: Vec<BranchID>,
        branches: Vec<PatternExecutor>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        debug_assert!(branch_ids.len() == branches.len());
        Self { branches, branch_ids, selected_variables, output_width }
    }

    pub(crate) fn reset(&mut self) {
        self.branches.iter_mut().for_each(|branch| branch.reset())
    }

    pub(crate) fn map_output(&self, source_branch_index: BranchIndex, unmapped: FixedBatch) -> FixedBatch {
        let mut uniform_batch = FixedBatch::new(self.output_width);
        unmapped.into_iter().for_each(|row| {
            uniform_batch.append(|mut output_row| {
                output_row.copy_mapped(row, self.selected_variables.iter().map(|&pos| (pos, pos)));
                output_row.set_branch_id_in_provenance(self.branch_ids[*source_branch_index]);
            })
        });
        uniform_batch
    }
}

#[derive(Debug)]
pub struct NegationExecutor {
    pub inner: PatternExecutor,
}

impl NegationExecutor {
    pub(crate) fn new(inner: PatternExecutor) -> Self {
        Self { inner }
    }

    pub(crate) fn reset(&mut self) {
        self.inner.reset()
    }
}

#[derive(Debug)]
pub struct InlinedCallExecutor {
    pub inner: PatternExecutor,
    pub arg_mapping: Vec<VariablePosition>,
    pub assignment_positions: Vec<Option<VariablePosition>>,
    pub output_width: u32,
    pub parameter_registry: Arc<ParameterRegistry>,
}

impl InlinedCallExecutor {
    pub(crate) fn new(
        inner: PatternExecutor,
        function_call: &FunctionCallStep,
        parameter_registry: Arc<ParameterRegistry>,
    ) -> Self {
        Self {
            inner,
            arg_mapping: function_call.arguments.clone(),
            assignment_positions: function_call.assigned.clone(),
            output_width: function_call.output_width,
            parameter_registry,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.inner.reset()
    }

    pub(crate) fn map_output(&self, input: MaybeOwnedRow<'_>, batch: FixedBatch) -> FixedBatch {
        let mut output_batch = FixedBatch::new(self.output_width);
        let check_indices: Vec<_> = self
            .assignment_positions
            .iter()
            .enumerate()
            .filter_map(|(src, &dst)| Some((VariablePosition::new(src as u32), dst?)))
            .filter(|(_src, dst)| dst.as_usize() < input.len() && input.get(*dst) != &VariableValue::None)
            .collect(); // TODO: Can we move this to compilation?
        for return_index in 0..batch.len() {
            let returned_row = batch.get_row(return_index);
            if check_indices.iter().all(|(src, dst)| returned_row.get(*src) == input.get(*dst)) {
                output_batch.append(|mut output_row| {
                    output_row.copy_from_row(input.as_reference());
                    output_row.copy_mapped(
                        returned_row.as_reference(),
                        self.assignment_positions
                            .iter()
                            .enumerate()
                            .filter_map(|(src, &dst)| Some((VariablePosition::new(src as u32), dst?))),
                    );
                    // Fix provenance:
                    output_row.set_provenance(input.provenance());
                });
            }
        }
        output_batch
    }
}

// from/into
impl From<NegationExecutor> for StepExecutors {
    fn from(value: NegationExecutor) -> Self {
        Self::Negation(value)
    }
}

impl From<DisjunctionExecutor> for StepExecutors {
    fn from(value: DisjunctionExecutor) -> Self {
        Self::Disjunction(value)
    }
}

impl From<InlinedCallExecutor> for StepExecutors {
    fn from(value: InlinedCallExecutor) -> Self {
        Self::InlinedCall(value)
    }
}
