/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use answer::variable_value::VariableValue;
use compiler::{VariablePosition, executable::match_::planner::conjunction_executable::FunctionCallStep};
use ir::{pattern::BranchID, pipeline::ParameterRegistry};

use crate::{
    batch::FixedBatch,
    read::{BranchIndex, pattern_executor::PatternExecutor, step_executor::StepExecutors},
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub struct DisjunctionExecutor {
    pub branches: Vec<PatternExecutor>,
    pub branch_ids: Vec<BranchID>,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
    unselected_positions: Vec<VariablePosition>,
}

impl DisjunctionExecutor {
    pub(crate) fn new(
        branch_ids: Vec<BranchID>,
        branches: Vec<PatternExecutor>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        debug_assert!(branch_ids.len() == branches.len());
        let unselected_positions = unselected_positions(&selected_variables, output_width);
        Self { branches, branch_ids, selected_variables, output_width, unselected_positions }
    }

    pub(crate) fn output_width(&self) -> u32 {
        self.output_width
    }

    pub(crate) fn reset(&mut self) {
        self.branches.iter_mut().for_each(|branch| branch.reset())
    }

    pub(crate) fn map_output(&self, source_branch_index: BranchIndex, unmapped: FixedBatch) -> FixedBatch {
        let branch_id = self.branch_ids[*source_branch_index];
        if unmapped.width() == self.output_width {
            return map_output_in_place(unmapped, &self.unselected_positions, branch_id);
        }
        let mut uniform_batch = FixedBatch::new(self.output_width);
        unmapped.into_iter().for_each(|row| {
            uniform_batch.append(|mut output_row| {
                output_row.copy_mapped(row, self.selected_variables.iter().map(|&pos| (pos, pos)));
                output_row.set_branch_id_in_provenance(branch_id);
            })
        });
        uniform_batch
    }
}

#[derive(Debug)]
pub struct OptionalExecutor {
    pub inner: PatternExecutor,
    pub branch_id: BranchID,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
    unselected_positions: Vec<VariablePosition>,
}

impl OptionalExecutor {
    pub(crate) fn new(
        branch_id: BranchID,
        inner: PatternExecutor,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        let unselected_positions = unselected_positions(&selected_variables, output_width);
        Self { inner, branch_id, selected_variables, output_width, unselected_positions }
    }

    pub(crate) fn output_width(&self) -> u32 {
        self.output_width
    }

    pub(crate) fn reset(&mut self) {
        self.inner.reset()
    }

    pub(crate) fn map_output(&self, unmapped: FixedBatch) -> FixedBatch {
        if unmapped.width() == self.output_width {
            return map_output_in_place(unmapped, &self.unselected_positions, self.branch_id);
        }
        let mut output = FixedBatch::new(self.output_width);
        unmapped.into_iter().for_each(|row| {
            output.append(|mut output_row| {
                output_row.copy_mapped(row, self.selected_variables.iter().map(|&pos| (pos, pos)));
                output_row.set_branch_id_in_provenance(self.branch_id);
            })
        });
        output
    }

    pub(crate) fn map_as_failed_output(&self, unmapped_input: MaybeOwnedRow<'_>) -> FixedBatch {
        let mut output = FixedBatch::new(self.output_width);
        output.append(|mut output_row| {
            output_row
                .copy_mapped(unmapped_input.as_reference(), self.selected_variables.iter().map(|&pos| (pos, pos)));
            output_row.set_provenance(unmapped_input.provenance()); // Pass through old provenance
        });
        output
    }
}

fn unselected_positions(selected_variables: &[VariablePosition], output_width: u32) -> Vec<VariablePosition> {
    (0..output_width).map(VariablePosition::new).filter(|position| !selected_variables.contains(position)).collect()
}

fn map_output_in_place(
    mut batch: FixedBatch,
    unselected_positions: &[VariablePosition],
    branch_id: BranchID,
) -> FixedBatch {
    for index in 0..batch.len() {
        let mut row = batch.get_row_mut(index);
        for &position in unselected_positions {
            row.unset(position);
        }
        row.set_branch_id_in_provenance(branch_id);
    }
    batch
}

#[derive(Debug)]
pub struct NegationExecutor {
    pub inner: PatternExecutor,
}

impl NegationExecutor {
    pub(crate) fn new(inner: PatternExecutor) -> Self {
        Self { inner }
    }

    pub(crate) fn output_width(&self) -> u32 {
        self.inner.output_width()
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

    pub(crate) fn output_width(&self) -> u32 {
        self.output_width
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

impl From<OptionalExecutor> for StepExecutors {
    fn from(value: OptionalExecutor) -> Self {
        Self::Optional(value)
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

#[cfg(test)]
mod tests {
    use answer::variable_value::VariableValue;
    use compiler::{VariablePosition, executable::next_executable_id};
    use encoding::value::value::Value;
    use ir::pattern::BranchID;

    use super::{DisjunctionExecutor, OptionalExecutor};
    use crate::{
        Provenance,
        batch::FixedBatch,
        read::{BranchIndex, pattern_executor::PatternExecutor},
    };

    const BRANCH: BranchID = BranchID(3);

    fn value(integer: Option<i64>) -> VariableValue<'static> {
        integer.map_or(VariableValue::None, |integer| VariableValue::Value(Value::Integer(integer)))
    }

    fn batch_of(rows: &[&[Option<i64>]]) -> FixedBatch {
        let mut batch = FixedBatch::new(rows[0].len() as u32);
        for (index, values) in rows.iter().enumerate() {
            batch.append(|mut row| {
                for (position, &integer) in values.iter().enumerate() {
                    row.set(VariablePosition::new(position as u32), value(integer));
                }
                row.set_multiplicity(index as u64 + 1);
                row.set_provenance(Provenance(index as u64 + 100));
            });
        }
        batch
    }

    fn assert_mapped(batch: &FixedBatch, expected: &[&[Option<i64>]]) {
        assert_eq!(batch.width(), expected[0].len() as u32);
        assert_eq!(batch.len(), expected.len() as u32);
        for (index, values) in expected.iter().enumerate() {
            let row = batch.get_row(index as u32);
            assert_eq!(row.row(), values.iter().map(|&integer| value(integer)).collect::<Vec<_>>().as_slice());
            assert_eq!(row.multiplicity(), index as u64 + 1);
            assert_eq!(row.provenance().0, (index as u64 + 100) | (1 << BRANCH.0));
        }
    }

    fn variable_positions(positions: &[u32]) -> Vec<VariablePosition> {
        positions.iter().copied().map(VariablePosition::new).collect()
    }

    fn disjunction(selected_variables: &[u32], output_width: u32) -> DisjunctionExecutor {
        let branch = PatternExecutor::new(next_executable_id(), Vec::new());
        DisjunctionExecutor::new(vec![BRANCH], vec![branch], variable_positions(selected_variables), output_width)
    }

    fn optional(selected_variables: &[u32], output_width: u32) -> OptionalExecutor {
        let inner = PatternExecutor::new(next_executable_id(), Vec::new());
        OptionalExecutor::new(BRANCH, inner, variable_positions(selected_variables), output_width)
    }

    #[test]
    fn disjunction_branch_output_of_the_step_width_keeps_selected_positions_and_unsets_the_rest() {
        let unmapped = batch_of(&[&[Some(1), Some(2), Some(3)], &[Some(4), None, Some(6)]]);
        let mapped = disjunction(&[0, 2], 3).map_output(BranchIndex(0), unmapped);
        assert_mapped(&mapped, &[&[Some(1), None, Some(3)], &[Some(4), None, Some(6)]]);
    }

    #[test]
    fn disjunction_branch_output_narrower_than_the_step_is_widened() {
        let unmapped = batch_of(&[&[Some(1), Some(2)], &[Some(4), None]]);
        let mapped = disjunction(&[0, 2], 3).map_output(BranchIndex(0), unmapped);
        assert_mapped(&mapped, &[&[Some(1), None, None], &[Some(4), None, None]]);
    }

    #[test]
    fn disjunction_branch_output_wider_than_the_step_is_narrowed() {
        let unmapped = batch_of(&[&[Some(1), Some(2), Some(3), Some(9)], &[Some(4), None, Some(6), Some(9)]]);
        let mapped = disjunction(&[0, 2], 3).map_output(BranchIndex(0), unmapped);
        assert_mapped(&mapped, &[&[Some(1), None, Some(3)], &[Some(4), None, Some(6)]]);
    }

    #[test]
    fn optional_output_of_the_step_width_keeps_selected_positions_and_unsets_the_rest() {
        let unmapped = batch_of(&[&[Some(1), Some(2), Some(3)], &[None, Some(5), Some(6)]]);
        let mapped = optional(&[1, 2], 3).map_output(unmapped);
        assert_mapped(&mapped, &[&[None, Some(2), Some(3)], &[None, Some(5), Some(6)]]);
    }

    #[test]
    fn optional_output_narrower_than_the_step_is_widened() {
        let unmapped = batch_of(&[&[Some(1), Some(2)], &[None, Some(5)]]);
        let mapped = optional(&[1, 2], 3).map_output(unmapped);
        assert_mapped(&mapped, &[&[None, Some(2), None], &[None, Some(5), None]]);
    }
}
