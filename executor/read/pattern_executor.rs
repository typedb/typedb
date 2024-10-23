/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        collecting_stage_executor::CollectedStageIterator, nested_pattern_executor::SubQueryResult,
        step_executor::StepExecutors,
    },
    ExecutionInterrupt,
};

#[derive(Copy, Clone)]
pub(super) struct BranchIndex(pub usize);

#[derive(Copy, Clone)]
pub(super) struct InstructionIndex(pub usize);

impl InstructionIndex {
    fn next(&self) -> InstructionIndex {
        InstructionIndex(self.0 + 1)
    }
}

pub(super) enum ControlInstruction {
    Start(FixedBatch),

    ExecuteImmediate(InstructionIndex),

    MapRowBatchToRowForNested(InstructionIndex, FixedBatchRowIterator),
    ExecuteNested(InstructionIndex, BranchIndex),

    CollectingStage(InstructionIndex),
    StreamCollected(InstructionIndex, CollectedStageIterator),

    Yield(FixedBatch),
}

pub(crate) struct PatternExecutor {
    executors: Vec<StepExecutors>,
    control_stack: Vec<ControlInstruction>,
}

impl PatternExecutor {
    pub(crate) fn new(executors: Vec<StepExecutors>) -> Self {
        PatternExecutor { executors, control_stack: Vec::new() }
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
        self.control_stack.push(ControlInstruction::Start(input_batch));
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
            let Self { control_stack, executors } = self;
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            match control_stack.pop().unwrap() {
                ControlInstruction::Start(batch) => {
                    self.prepare_next_instruction_and_push_to_stack(context, InstructionIndex(0), batch)?;
                }
                ControlInstruction::ExecuteImmediate(index) => {
                    let executor = executors[index.0].unwrap_immediate();
                    if let Some(batch) = executor.batch_continue(context, interrupt)? {
                        control_stack.push(ControlInstruction::ExecuteImmediate(index));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::MapRowBatchToRowForNested(index, mut iter) => {
                    if let Some(row_result) = iter.next() {
                        let unmapped_input = row_result.unwrap().into_owned();
                        control_stack.push(ControlInstruction::MapRowBatchToRowForNested(index, iter));
                        let nested_executor = executors[index.0].unwrap_branch();
                        let (branches, mut controller) = nested_executor.to_parts_mut();
                        let row = controller.prepare_and_map_input(&unmapped_input);
                        for (branch_index, pattern) in branches.iter_mut().enumerate() {
                            pattern.prepare(row.clone().into_owned());
                            control_stack
                                .push(ControlInstruction::ExecuteNested(index, BranchIndex(branch_index)));
                        }
                    }
                }
                ControlInstruction::ExecuteNested(index, branch_index) => {
                    // TODO: This bit desperately needs a cleanup.
                    let (branches, controller) = &mut executors[index.0].unwrap_branch().to_parts_mut();
                    let branch = &mut branches[branch_index.0];
                    let unmapped = branch.pattern_executor.batch_continue(context, interrupt)?;
                    let found = match controller.map_output(branch.input.as_ref().unwrap(), unmapped) {
                        SubQueryResult::Retry(found) => {
                            control_stack.push(ControlInstruction::ExecuteNested(index, branch_index));
                            found
                        }
                        SubQueryResult::Done(found) => {
                            branch.pattern_executor.reset();
                            found
                        }
                    };
                    if let Some(batch) = found {
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::CollectingStage(index) => {
                    let (pattern, mut collector) = executors[index.0].unwrap_collecting_stage().to_parts_mut();
                    match pattern.compute_next_batch(context, interrupt)? {
                        Some(batch) => {
                            collector.accept(context, batch);
                            self.control_stack.push(ControlInstruction::CollectingStage(index))
                        }
                        None => {
                            let iterator = collector.into_iterator();
                            self.control_stack.push(ControlInstruction::StreamCollected(index, iterator))
                        }
                    }
                }
                ControlInstruction::StreamCollected(index, mut collected_iterator) => {
                    if let Some(batch) = collected_iterator.batch_continue()? {
                        self.control_stack.push(ControlInstruction::StreamCollected(index, collected_iterator));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::Yield(batch) => {
                    return Ok(Some(batch));
                }
            }
        }
        Ok(None) // Nothing in the stack
    }

    fn prepare_next_instruction_and_push_to_stack(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        next_instruction_index: InstructionIndex,
        batch: FixedBatch,
    ) -> Result<(), ReadExecutionError> {
        if next_instruction_index.0 >= self.executors.len() {
            self.control_stack.push(ControlInstruction::Yield(batch));
        } else {
            match &mut self.executors[next_instruction_index.0] {
                StepExecutors::Immediate(executable) => {
                    executable.prepare(batch, context)?;
                    self.control_stack.push(ControlInstruction::ExecuteImmediate(next_instruction_index));
                }
                StepExecutors::Nested(_) => self
                    .control_stack
                    .push(ControlInstruction::MapRowBatchToRowForNested(next_instruction_index, batch.into_iterator())),
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    self.control_stack.push(ControlInstruction::CollectingStage(next_instruction_index));
                }
                StepExecutors::ReshapeForReturn(_) => todo!(),
            }
        }
        Ok(())
    }
}
