/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::MatchExecutable;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        step_executor::{create_executors_recursive, StepExecutors},
        nested_pattern_executor::{BaseNestedPatternExecutor, NestedPatternController, NestedPatternExecutor},
    },
    ExecutionInterrupt,
};

pub(super) struct BranchIndex(pub usize);
pub(super) struct InstructionIndex(pub usize);

pub(super) enum ControlInstruction {
    Start(Option<FixedBatch>),
    Execute(InstructionIndex),
    NestedBranch(InstructionIndex, BranchIndex),
}

impl ControlInstruction {
    pub(crate) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        pattern_instructions: &mut Vec<StepExecutors>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            ControlInstruction::Start(batch_opt) => Ok(batch_opt.take()),
            ControlInstruction::Execute(InstructionIndex(index)) => {
                pattern_instructions[*index].unwrap_executable().batch_continue(context, interrupt)
            }
            ControlInstruction::NestedBranch(InstructionIndex(index), BranchIndex(branch_index)) => {
                match pattern_instructions[*index].unwrap_nested_pattern_branch() {
                    NestedPatternExecutor::Negation(negation) => {
                        debug_assert!(*branch_index == 0);
                        PatternExecutor::execute_nested_pattern(context, interrupt, negation)
                    }
                    NestedPatternExecutor::Disjunction(branches) => {
                        PatternExecutor::execute_nested_pattern(context, interrupt, &mut branches[*branch_index])
                    }
                }
            }
        }
    }
}

impl ControlInstruction {
    pub(crate) fn next_index(&self) -> InstructionIndex {
        match self {
            ControlInstruction::Start(_) => InstructionIndex(0),
            ControlInstruction::Execute(InstructionIndex(idx)) => InstructionIndex(idx + 1),
            ControlInstruction::NestedBranch(InstructionIndex(idx), _) => InstructionIndex(idx + 1),
        }
    }
}

pub(crate) struct PatternExecutor {
    executors: Vec<StepExecutors>,
    control_stack: Vec<ControlInstruction>,
}

impl PatternExecutor {
    pub(crate) fn build(
        match_executable: &MatchExecutable,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let instructions = create_executors_recursive(match_executable, snapshot, thing_manager)?;
        Ok(PatternExecutor { executors: instructions, control_stack: Vec::new() })
    }

    pub(crate) fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        self.batch_continue(context, interrupt)
    }

    pub(crate) fn prepare(&mut self, input_batch: FixedBatch) {
        debug_assert!(self.control_stack.is_empty());
        self.reset();
        self.control_stack.push(ControlInstruction::Start(Some(input_batch)));
    }

    pub(super) fn reset(&mut self) {
        self.control_stack.clear();
    }

    pub(super) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(self.control_stack.len() > 0);
        while self.control_stack.last().is_some() {
            let Self { control_stack: stack, executors: instructions } = self;
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }
            let mut step = stack.last_mut().unwrap();
            let step_result = step.batch_continue(context, interrupt, instructions)?;

            if let Some(batch) = step_result {
                let InstructionIndex(next_index) = step.next_index();
                if next_index >= instructions.len() {
                    return Ok(Some(batch));
                } else {
                    let next_index = step.next_index();
                    self.prepare_and_push_to_stack(context, next_index, batch)?;
                }
            } else {
                self.pop_stack();
            }
        }
        Ok(None) // Nothing in the stack
    }

    fn execute_nested_pattern(
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        executor: &mut BaseNestedPatternExecutor<impl NestedPatternController>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        while let Some(pattern_executor) = executor.get_or_next_executing_pattern() {
            let result = pattern_executor.batch_continue(context, interrupt)?;
            if let Some(batch) = executor.process_result(result) {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    fn prepare_and_push_to_stack(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        index: InstructionIndex,
        batch: FixedBatch,
    ) -> Result<(), ReadExecutionError> {
        let InstructionIndex(index) = index;
        match &mut self.executors[index] {
            StepExecutors::Immediate(executable) => {
                executable.prepare(batch, context)?;
                self.control_stack.push(ControlInstruction::Execute(InstructionIndex(index)));
            }
            StepExecutors::NestedPattern(nested) => {
                nested.prepare_all_branches(batch, context)?;
                for i in 0..nested.branch_count() {
                    self.control_stack.push(ControlInstruction::NestedBranch(InstructionIndex(index), BranchIndex(i)))
                }
            }
        }
        Ok(())
    }

    pub(super) fn stack_top(&mut self) -> Option<&mut ControlInstruction> {
        self.control_stack.last_mut()
    }

    fn pop_stack(&mut self) {
        self.control_stack.pop(); // Just in case we need to do more here, like copy suspend points
    }
}
