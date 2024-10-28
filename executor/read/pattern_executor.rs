/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::DerefMut;

use compiler::VariablePosition;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    ExecutionInterrupt,
    pipeline::stage::ExecutionContext,
    read::{
        nested_pattern_executor::{
            InlinedFunctionMapper, LimitMapper, NegationMapper, NestedPatternExecutor, NestedPatternResultMapper,
            OffsetMapper, SelectMapper,
        },
        step_executor::StepExecutors,
        SuspendPoint,
        tabled_call_executor::TabledCallResult,
        tabled_functions::{TabledFunctionPatternExecutorState, TabledFunctions},
    },
    row::MaybeOwnedRow,
};
use crate::read::control_instruction::{CollectingStage, ControlInstruction, ExecuteImmediate, ExecuteNested, MapRowBatchToRowForNested, ReshapeForReturn, PatternStart, StreamCollected, TabledCall, Yield};


#[derive(Debug, Copy, Clone)]
pub(super) struct BranchIndex(pub usize);

#[derive(Debug, Copy, Clone)]
pub(super) struct ExecutorIndex(pub usize);

impl ExecutorIndex {
    fn next(&self) -> ExecutorIndex {
        ExecutorIndex(self.0 + 1)
    }
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
        self.control_stack.push(ControlInstruction::PatternStart(PatternStart { input_batch }));
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
                ControlInstruction::PatternStart(PatternStart { input_batch }) => {
                    self.prepare_next_instruction_and_push_to_stack(context, ExecutorIndex(0), input_batch)?;
                }
                ControlInstruction::ExecuteImmediate(ExecuteImmediate { index }) => {
                    let executor = executors[index.0].unwrap_immediate();
                    if let Some(batch) = executor.batch_continue(context, interrupt)? {
                        control_stack.push(ControlInstruction::ExecuteImmediate(ExecuteImmediate { index }));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested { index, mut iterator }) => {
                    if let Some(row_result) = iterator.next() {
                        let unmapped_input = row_result.unwrap().into_owned();
                        control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested { index, iterator }));
                        match &mut executors[index.0] {
                            StepExecutors::Nested(_) => {
                                self.prepare_and_push_nested_pattern(index, unmapped_input);
                            }
                            StepExecutors::TabledCall(tabled_call_executor) => {
                                tabled_call_executor.prepare(unmapped_input);
                                control_stack.push(ControlInstruction::TabledCall(TabledCall { index }));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                ControlInstruction::ExecuteNested(ExecuteNested { index, branch_index, mut mapper, input, parameters_override }) => {
                    // TODO: This bit desperately needs a cleanup.
                    let branch = &mut executors[index.0].unwrap_branch().get_branch(branch_index);
                    let suspend_point_len_before = suspend_point_accumulator.len();
                    let nested_context = if let Some(parameters) = &parameters_override {
                        ExecutionContext::new(
                            context.snapshot.clone(),
                            context.thing_manager.clone(),
                            parameters.clone(),
                        )
                    } else {
                        ExecutionContext::new(
                            context.snapshot.clone(),
                            context.thing_manager.clone(),
                            context.parameters.clone(),
                        )
                    };
                    let unmapped = branch.batch_continue(
                        &nested_context,
                        interrupt,
                        tabled_functions,
                        suspend_point_accumulator,
                    )?;
                    if suspend_point_accumulator.len() != suspend_point_len_before {
                        suspend_point_accumulator.push(SuspendPoint::new_nested(index, branch_index, input.clone()))
                    }
                    let (must_retry, mapped) = mapper.map_output(unmapped).into_parts();
                    if must_retry {
                        control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                            index,
                            branch_index,
                            mapper,
                            input,
                            parameters_override,
                        }));
                    } else {
                        branch.reset();
                    }
                    if let Some(batch) = mapped {
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::TabledCall(TabledCall { index }) => {
                    let executor = executors[index.0].unwrap_tabled_call();
                    let call_key = executor.active_call_key().unwrap();
                    let function_state = tabled_functions.get_or_create_function_state(context, call_key)?;
                    let found = match executor.try_read_next_batch(&function_state) {
                        TabledCallResult::RetrievedFromTable(batch) => Some(batch),
                        TabledCallResult::Suspend => {
                            suspend_point_accumulator.push(executor.create_suspend_point_for(index));
                            None
                        }
                        TabledCallResult::MustExecutePattern(mut pattern_state_mutex_guard) => {
                            let TabledFunctionPatternExecutorState { pattern_executor, suspend_points, parameters } =
                                pattern_state_mutex_guard.deref_mut();
                            let context_with_function_parameters = ExecutionContext::new(
                                context.snapshot.clone(),
                                context.thing_manager.clone(),
                                parameters.clone(),
                            );
                            let batch_opt = pattern_executor.batch_continue(
                                &context_with_function_parameters,
                                interrupt,
                                tabled_functions,
                                suspend_points,
                            )?;
                            if let Some(batch) = batch_opt {
                                let deduplicated_batch = executor.add_batch_to_table(&function_state, batch);
                                Some(deduplicated_batch)
                            } else {
                                if !suspend_points.is_empty() {
                                    suspend_point_accumulator.push(executor.create_suspend_point_for(index))
                                }
                                None
                            }
                        }
                    };
                    if let Some(batch) = found {
                        self.control_stack.push(ControlInstruction::TabledCall(TabledCall { index: index.clone() }));
                        let mapped = executor.map_output(batch);
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), mapped)?;
                    } else {
                        // TODO: This looks like the place to consider a retry?
                    }
                }
                ControlInstruction::CollectingStage(CollectingStage { index }) => {
                    let (pattern, mut collector) = executors[index.0].unwrap_collecting_stage().to_parts_mut();
                    // Distinct isn't a collecting stage. We should use fresh suspend_point_accumulators here.
                    match pattern.batch_continue(context, interrupt, tabled_functions, suspend_point_accumulator)? {
                        Some(batch) => {
                            collector.accept(context, batch);
                            self.control_stack.push(ControlInstruction::CollectingStage(CollectingStage { index }))
                        }
                        None => {
                            let iterator = collector.into_iterator();
                            self.control_stack.push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }))
                        }
                    }
                }
                ControlInstruction::StreamCollected(StreamCollected { index, mut iterator }) => {
                    if let Some(batch) = iterator.batch_continue()? {
                        self.control_stack.push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ReshapeForReturn(ReshapeForReturn { index, to_reshape: batch }) => {
                    let return_positions = executors[index.0].unwrap_reshape();
                    let mut output_batch = FixedBatch::new(return_positions.len() as u32);
                    for row in batch {
                        output_batch.append(|mut write_to| {
                            return_positions.iter().enumerate().for_each(|(dst, src)| {
                                write_to.set(VariablePosition::new(dst as u32), row.get(src.clone()).clone().to_owned())
                            })
                        })
                    }
                    self.prepare_next_instruction_and_push_to_stack(context, index.next(), output_batch)?;
                }
                ControlInstruction::Yield(Yield { batch }) => {
                    return Ok(Some(batch));
                }
            }
        }
        Ok(None) // Nothing in the stack
    }

    fn prepare_next_instruction_and_push_to_stack(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        next_executor_index: ExecutorIndex,
        batch: FixedBatch,
    ) -> Result<(), ReadExecutionError> {
        if next_executor_index.0 >= self.executors.len() {
            self.control_stack.push(ControlInstruction::Yield(Yield { batch }));
        } else {
            match &mut self.executors[next_executor_index.0] {
                StepExecutors::Immediate(executable) => {
                    executable.prepare(batch, context)?;
                    self.control_stack.push(ControlInstruction::ExecuteImmediate(ExecuteImmediate { index: next_executor_index }));
                }
                StepExecutors::Nested(_) => self.control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested {
                    index: next_executor_index,
                    iterator: FixedBatchRowIterator::new(Ok(batch)),
                })),
                StepExecutors::TabledCall(_) => {
                    self.control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested {
                        index: next_executor_index,
                        iterator: FixedBatchRowIterator::new(Ok(batch)),
                    }));
                }
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    self.control_stack.push(ControlInstruction::CollectingStage(CollectingStage { index: next_executor_index }));
                }
                StepExecutors::ReshapeForReturn(_) => {
                    self.control_stack
                        .push(ControlInstruction::ReshapeForReturn(ReshapeForReturn { index: next_executor_index, to_reshape: batch }));
                }
            }
        }
        Ok(())
    }

    fn prepare_and_push_nested_pattern(&mut self, index: ExecutorIndex, input: MaybeOwnedRow<'_>) {
        let executor = self.executors[index.0].unwrap_branch();
        match executor {
            NestedPatternExecutor::Disjunction { branches, selected_variables, output_width } => {
                for (branch_index, branch) in branches.iter_mut().enumerate() {
                    let mut mapper =
                        NestedPatternResultMapper::Select(SelectMapper::new(selected_variables.clone(), *output_width));
                    let mapped_input = mapper.map_input(&input);
                    branch.prepare(FixedBatch::from(mapped_input));
                    self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                        index,
                        branch_index: BranchIndex(branch_index),
                        mapper,
                        input: input.clone().into_owned(),
                        parameters_override: None,
                    }))
                }
            }
            NestedPatternExecutor::Negation { inner } => {
                let mut mapper = NestedPatternResultMapper::Negation(NegationMapper::new(input.clone().into_owned()));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                    index,
                    branch_index: BranchIndex(0),
                    mapper,
                    input: input.clone().into_owned(),
                    parameters_override: None,
                }));
            }
            NestedPatternExecutor::InlinedFunction {
                inner,
                arg_mapping,
                return_mapping,
                output_width,
                parameter_registry,
            } => {
                let mut mapper = NestedPatternResultMapper::InlinedFunction(InlinedFunctionMapper::new(
                    input.clone().into_owned(),
                    arg_mapping.clone(),
                    return_mapping.clone(),
                    *output_width,
                ));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                    index,
                    branch_index: BranchIndex(0),
                    mapper,
                    input: input.clone().into_owned(),
                    parameters_override: Some(parameter_registry.clone()),
                }));
            }
            NestedPatternExecutor::Offset { inner, offset } => {
                let mut mapper = NestedPatternResultMapper::Offset(OffsetMapper::new(*offset));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                    index,
                    branch_index: BranchIndex(0),
                    mapper,
                    input: input.clone().into_owned(),
                    parameters_override: None,
                }));
            }
            NestedPatternExecutor::Limit { inner, limit } => {
                let mut mapper = NestedPatternResultMapper::Limit(LimitMapper::new(*limit));
                let mapped_input = mapper.map_input(&input);
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                    index,
                    branch_index: BranchIndex(0),
                    mapper,
                    input: input.clone().into_owned(),
                    parameters_override: None,
                }));
            }
        }
    }
}
