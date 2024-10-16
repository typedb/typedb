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
        nested_pattern_executor::{BaseNestedPatternExecutor, NestedPatternController, NestedPatternExecutor},
        step_executor::{create_executors_recursive, StepExecutors},
    },
    ExecutionInterrupt,
};

pub(super) struct BranchIndex(pub usize);
pub(super) struct InstructionIndex(pub usize);

pub(super) enum StackInstruction {
    Start(Option<FixedBatch>),
    Execute(InstructionIndex),
    NestedPatternBranch(InstructionIndex, BranchIndex),
}

impl StackInstruction {
    pub(crate) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        pattern_instructions: &mut Vec<StepExecutors>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            StackInstruction::Start(batch_opt) => Ok(batch_opt.take()),
            StackInstruction::Execute(InstructionIndex(idx)) => {
                pattern_instructions[*idx].unwrap_executable().batch_continue(context, interrupt)
            }
            StackInstruction::NestedPatternBranch(InstructionIndex(idx), BranchIndex(branch_index)) => {
                match pattern_instructions[*idx].unwrap_nested_pattern_branch() {
                    NestedPatternExecutor::Negation(negation) => {
                        debug_assert!(*branch_index == 0);
                        PatternExecutor::execute_nested_pattern(context, interrupt, negation)
                    }
                    NestedPatternExecutor::Disjunction(branches) => {
                        PatternExecutor::execute_nested_pattern(context, interrupt, &mut branches[*branch_index])
                    }
                    NestedPatternExecutor::InlinedFunction(body) => {
                        debug_assert!(*branch_index == 0);
                        PatternExecutor::execute_nested_pattern(context, interrupt, body)
                    }
                }
            }
        }
    }
}

impl StackInstruction {
    pub(crate) fn next_index(&self) -> InstructionIndex {
        match self {
            StackInstruction::Start(_) => InstructionIndex(0),
            StackInstruction::Execute(InstructionIndex(idx)) => InstructionIndex(idx + 1),
            StackInstruction::NestedPatternBranch(InstructionIndex(idx), _) => InstructionIndex(idx + 1),
        }
    }
}

pub(crate) struct PatternExecutor {
    instructions: Vec<StepExecutors>,
    stack: Vec<StackInstruction>,
}

impl PatternExecutor {
    pub(crate) fn build(
        match_executable: &MatchExecutable,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let instructions = create_executors_recursive(match_executable, snapshot, thing_manager)?;
        Ok(PatternExecutor { instructions, stack: Vec::new() })
    }

    pub(crate) fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        self.batch_continue(context, interrupt)
    }

    pub(crate) fn prepare(&mut self, input_batch: FixedBatch) {
        debug_assert!(self.stack.is_empty());
        self.reset();
        self.stack.push(StackInstruction::Start(Some(input_batch)));
    }

    pub(super) fn reset(&mut self) {
        self.stack.clear();
    }

    pub(super) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(self.stack.len() > 0);
        while self.stack.last().is_some() {
            let Self { stack, instructions } = self;
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
        match &mut self.instructions[index] {
            StepExecutors::Immediate(executable) => {
                executable.prepare(batch, context)?;
                self.stack.push(StackInstruction::Execute(InstructionIndex(index)));
            }
            StepExecutors::NestedPattern(nested) => {
                nested.prepare_all_branches(batch, context)?;
                for i in 0..nested.branch_count() {
                    self.stack.push(StackInstruction::NestedPatternBranch(InstructionIndex(index), BranchIndex(i)))
                }
            }
        }
        Ok(())
    }

    pub(super) fn stack_top(&mut self) -> Option<&mut StackInstruction> {
        self.stack.last_mut()
    }

    fn pop_stack(&mut self) {
        self.stack.pop(); // Just in case we need to do more here, like copy suspend points
    }
}
