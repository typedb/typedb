/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{ops::DerefMut, sync::TryLockError};
use std::iter::zip;

use itertools::Either;
use compiler::VariablePosition;
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
        step_executor::StepExecutors, tabled_functions::TabledFunctions,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use crate::read::SuspendPoint;
use crate::read::tabled_functions::{TabledFunctionPatternExecutorState};

#[derive(Debug, Copy, Clone)]
pub(super) struct BranchIndex(pub usize);

#[derive(Debug, Copy, Clone)]
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
    ExecuteNested(InstructionIndex, BranchIndex, NestedPatternResultMapper, MaybeOwnedRow<'static>),

    TabledCall(InstructionIndex), // TODO: Use a FunctionMapper

    CollectingStage(InstructionIndex),
    StreamCollected(InstructionIndex, CollectedStageIterator),
    ReshapeForReturn(InstructionIndex, FixedBatch),

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
        suspend_points: &mut Vec<SuspendPoint>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        self.batch_continue(context, interrupt, tabled_functions, suspend_points)
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
        tabled_functions: &mut TabledFunctions,
        suspend_point_accumulator: &mut Vec<SuspendPoint>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        // debug_assert!(self.control_stack.len() > 0);
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
                        match &mut executors[index.0] {
                            StepExecutors::Nested(_) => {
                                self.prepare_and_push_nested_pattern(index, unmapped_input);
                            }
                            StepExecutors::TabledCall(tabled_call_executor) => {
                                tabled_call_executor.prepare(unmapped_input);
                                control_stack.push(ControlInstruction::TabledCall(index));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                ControlInstruction::ExecuteNested(index, branch_index, mut mapper, input) => {
                    // TODO: This bit desperately needs a cleanup.
                    let branch = &mut executors[index.0].unwrap_branch().get_branch(branch_index);
                    let suspend_point_len_before = suspend_point_accumulator.len();
                    let unmapped = branch.batch_continue(context, interrupt, tabled_functions, suspend_point_accumulator)?;
                    if suspend_point_accumulator.len() != suspend_point_len_before {
                        suspend_point_accumulator.push(SuspendPoint::new_nested(index, branch_index, input.clone()))
                    }
                    let (must_retry, mapped) = mapper.map_output(unmapped).into_parts();
                    if must_retry {
                        control_stack.push(ControlInstruction::ExecuteNested(index, branch_index, mapper, input.clone()));
                    } else {
                        branch.reset();
                    }
                    if let Some(batch) = mapped {
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::TabledCall(index) => {
                    let executor = executors[index.0].unwrap_tabled_call();
                    let call_key = executor.active_call_key().unwrap();
                    let mut function_state = tabled_functions.get_or_create_function_state(context, call_key)?;
                    let found = match executor.batch_continue_or_function_pattern(&function_state) {
                        Either::Left(batch) => {
                            eprintln!("fn({}): Serviced from table.", executor.active_call_key().unwrap().arguments);
                            Some(batch)
                        },
                        Either::Right(pattern_mutex) => {
                            match pattern_mutex.try_lock() {
                                Ok(mut executor_state) => {
                                    eprintln!("ENTER fn({}) {{", executor.active_call_key().unwrap().arguments);
                                    let TabledFunctionPatternExecutorState { pattern_executor, suspend_points } = executor_state.deref_mut();
                                    let batch_opt = pattern_executor.batch_continue(context, interrupt, tabled_functions, suspend_points)?;
                                    eprintln!("}} EXIT fn({}) with {:?} rows.", executor.active_call_key().unwrap().arguments, batch_opt.as_ref().map(|b| b.len()));
                                    if let Some(batch) = &batch_opt {
                                        // eprintln!("Adding to table for: fn({})", &executor.active_call_key().unwrap().arguments);
                                        executor.add_batch_to_table(&function_state, batch);
                                    } else {
                                        if !suspend_points.is_empty() {
                                            suspend_point_accumulator.push(executor.create_suspend_point_for(index))
                                        }
                                    }
                                    batch_opt
                                }
                                Err(TryLockError::WouldBlock) => {
                                    suspend_point_accumulator.push(executor.create_suspend_point_for(index));
                                    None
                                }
                                Err(TryLockError::Poisoned(_)) => {
                                    let call_key = executor.active_call_key().unwrap();
                                    return Err(ReadExecutionError::TabledFunctionLockError {
                                        function_id: call_key.function_id.clone(),
                                        arguments: call_key.arguments.clone(),
                                    });
                                }
                            }
                        }
                    };
                    if let Some(batch) = found {
                        self.control_stack.push(ControlInstruction::TabledCall(index.clone()));
                        eprintln!("fn({})  should have passed on:", executor.active_call_key().unwrap().arguments);
                        for i in 0..batch.len() {
                            eprintln!(" - {}", batch.get_row(i as u32));
                        }
                        let mapped = executor.map_output(batch);
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), mapped)?;
                    } else {
                        // TODO: This looks like the place to consider a retry?
                    }

                }
                ControlInstruction::CollectingStage(index) => {
                    let (pattern, mut collector) = executors[index.0].unwrap_collecting_stage().to_parts_mut();
                    // Distinct isn't a collecting stage. We should use fresh suspend_point_accumulators here.
                    match pattern.batch_continue(context, interrupt, tabled_functions, suspend_point_accumulator)? {
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
                ControlInstruction::ReshapeForReturn(index, batch) => {
                    let return_positions = executors[index.0].unwrap_reshape();
                    let mut batch_iter = batch.into_iterator();
                    let mut output_batch = FixedBatch::new(return_positions.len() as u32);
                    while let Some(row_result) = batch_iter.next() {
                        let row = row_result.unwrap();
                        output_batch.append(|mut write_to| {
                            return_positions.iter().enumerate().for_each(|(dst, src)| {
                                write_to.set(VariablePosition::new(dst as u32), row.get(src.clone()).clone().to_owned())
                            })
                        })
                    }
                    self.prepare_next_instruction_and_push_to_stack(context, index.next(), output_batch)?;
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
                StepExecutors::Nested(_) => self.control_stack.push(
                    ControlInstruction::MapRowBatchToRowForNested(next_instruction_index, batch.into_iterator())
                ),
                StepExecutors::TabledCall(_) => {
                    self.control_stack.push(ControlInstruction::MapRowBatchToRowForNested(
                        next_instruction_index,
                        batch.into_iterator(),
                    ));
                }
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    self.control_stack.push(ControlInstruction::CollectingStage(next_instruction_index));
                }
                StepExecutors::ReshapeForReturn(_) => {
                    self.control_stack.push(ControlInstruction::ReshapeForReturn(next_instruction_index, batch))

                },
            }
        }
        Ok(())
    }
    fn prepare_and_push_nested_pattern(&mut self, index: InstructionIndex, input: MaybeOwnedRow<'_>) {
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
                        input.clone().into_owned(),
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
                    input.clone().into_owned(),
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
                    mapper,
                    input.clone().into_owned(),
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
                    input.clone().into_owned(),
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
                    input.clone().into_owned(),
                ));
            }
        }
    }
}
