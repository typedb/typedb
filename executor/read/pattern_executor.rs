/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::DerefMut;
use std::sync::TryLockError;
use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        collecting_stage_executor::CollectedStageIterator,
        nested_pattern_executor::{
            IdentityMapper, InlinedFunctionMapper, LimitMapper, NegationMapper, NestedPatternExecutor,
            NestedPatternResultMapper, OffsetMapper,
        },
        step_executor::StepExecutors,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use crate::read::tabled_functions::TabledFunctions;

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
    ExecuteNested(InstructionIndex, BranchIndex, NestedPatternResultMapper),

    TabledCall(InstructionIndex),

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
        tabled_functions: &mut TabledFunctions,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        self.batch_continue(context, interrupt, tabled_functions)
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
        tabled_functions: &mut TabledFunctions
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
                        self.prepare_nested_pattern(index, unmapped_input);
                    }
                }
                ControlInstruction::ExecuteNested(index, branch_index, mut mapper) => {
                    // TODO: This bit desperately needs a cleanup.
                    let branch = &mut executors[index.0].unwrap_branch().get_branch(branch_index);
                    let unmapped = branch.batch_continue(context, interrupt)?;
                    let (must_retry, mapped) = mapper.map_output(unmapped).into_parts();
                    if must_retry {
                        control_stack.push(ControlInstruction::ExecuteNested(index, branch_index, mapper));
                    } else {
                        branch.reset();
                    }
                    if let Some(batch) = mapped {
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::TabledCall(index) => {
                    let step = executors[index.0].unwrap_tabled_call();
                    let call_key = step.active_call_key().unwrap();
                    let mutex =  tabled_functions.get_or_create_state_mutex(call_key);
                    let lock_result = mutex.try_lock();
                    match lock_result {
                        Ok(mut guard) => {
                            let found = match step.batch_continue_or_function_pattern(guard.deref_mut()) {
                                Either::Left(batch) => Some(batch),
                                Either::Right(pattern) => pattern.batch_continue(context, interrupt, tabled_functions)?
                            };
                            if let Some(batch) = found {
                                self.control_stack.push(ControlInstruction::TabledCall(index.clone()));
                                self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                            } else {
                                // TODO: This looks like the place to consider a retry?
                            }
                        },
                        Err(TryLockError::WouldBlock) => {
                            todo!("Cyclic call -> Suspend!")
                        },
                        Err(TryLockError::Poisoned(err)) => {
                            return Err(ReadExecutionError::TabledFunctionLockError { function_id: call_key.function_id.clone(), arguments: call_key.arguments.clone() });
                        }
                    }
                }
                ControlInstruction::CollectingStage(index) => {
                    let (pattern, mut collector) = executors[index.0].unwrap_collecting_stage().to_parts_mut();
                    match pattern.compute_next_batch(context, interrupt, tabled_functions)? {
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
                StepExecutors::TabledCall(_) => self.control_stack.push(ControlInstruction::TabledCall(next_instruction_index)),
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    self.control_stack.push(ControlInstruction::CollectingStage(next_instruction_index));
                }
                StepExecutors::ReshapeForReturn(_) => todo!(),
            }
        }
        Ok(())
    }
    fn prepare_nested_pattern(&mut self, index: InstructionIndex, input: MaybeOwnedRow<'_>) {
        let executor = self.executors[index.0].unwrap_branch();
        match executor {
            NestedPatternExecutor::Disjunction { branches } => {
                for (branch_index, branch) in branches.iter_mut().enumerate() {
                    let mut mapper = NestedPatternResultMapper::Identity(IdentityMapper);
                    let mapped_input = mapper.map_input(&input);
                    branch.prepare(FixedBatch::from(mapped_input));
                    self.control_stack.push(ControlInstruction::ExecuteNested(
                        index,
                        BranchIndex(branch_index),
                        mapper,
                    ))
                }
            }
            NestedPatternExecutor::Negation { inner } => {
                let mut mapper = NestedPatternResultMapper::Negation(NegationMapper::new(input.clone().into_owned()));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(
                    index,
                    BranchIndex(0),
                    mapper,
                ));
            }
            NestedPatternExecutor::InlinedFunction { inner, arg_mapping, return_mapping, output_width } => {
                let mut mapper = NestedPatternResultMapper::InlinedFunction(InlinedFunctionMapper::new(
                    input.clone().into_owned(),
                    arg_mapping.clone(),
                    return_mapping.clone(),
                    *output_width,
                ));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(
                    index,
                    BranchIndex(0),
                    mapper
                ));
            }
            NestedPatternExecutor::Offset { inner, offset } => {
                let mut mapper = NestedPatternResultMapper::Offset(OffsetMapper::new(*offset));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(
                    index,
                    BranchIndex(0),
                    mapper,
                ));
            }
            NestedPatternExecutor::Limit { inner, limit } => {
                let mut mapper = NestedPatternResultMapper::Limit(LimitMapper::new(*limit));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(
                    index,
                    BranchIndex(0),
                    mapper,
                ));
            }
        }
    }
}
