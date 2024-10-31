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
    pipeline::stage::ExecutionContext,
    read::{
        control_instruction::{
            CollectingStage, ControlInstruction, ExecuteDisjunction, ExecuteImmediate, ExecuteInlinedFunction,
            ExecuteNegation, ExecuteNested, MapRowBatchToRowForNested, PatternStart, ReshapeForReturn, StreamCollected,
            TabledCall, Yield,
        },
        nested_pattern_executor::{
            Disjunction, InlinedFunction, LimitMapper, Negation, NestedPatternExecutor, NestedPatternResultMapper,
            OffsetMapper,
        },
        step_executor::StepExecutors,
        tabled_call_executor::TabledCallResult,
        tabled_functions::{TabledFunctionPatternExecutorState, TabledFunctions},
        SuspendPoint,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

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
                        control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested {
                            index,
                            iterator,
                        }));
                        self.prepare_and_push_nested_pattern(index, unmapped_input);
                    }
                }
                ControlInstruction::ExecuteNegation(ExecuteNegation { index, input }) => {
                    let StepExecutors::Nested(NestedPatternExecutor::Negation(Negation { inner })) =
                        &mut executors[index.0]
                    else {
                        unreachable!();
                    };
                    let mut fresh_suspend_points = Vec::new();
                    match inner.batch_continue(context, interrupt, tabled_functions, &mut fresh_suspend_points)? {
                        None => self.prepare_next_instruction_and_push_to_stack(
                            context,
                            index.next(),
                            FixedBatch::from(input.as_reference()),
                        )?,
                        Some(_) => {
                            inner.reset();
                        } // fail
                    };
                    if !fresh_suspend_points.is_empty() {
                        // TODO: This goes away because we can always retry here.
                        suspend_point_accumulator.push(SuspendPoint::new_nested(
                            index,
                            BranchIndex(0),
                            input.clone().into_owned(),
                        ));
                    }
                }
                ControlInstruction::ExecuteDisjunction(ExecuteDisjunction { index, branch_index, input }) => {
                    let NestedPatternExecutor::Disjunction(disjunction) = &mut executors[index.0].unwrap_nested()
                    else {
                        unreachable!();
                    };
                    let branch = &mut disjunction.branches[branch_index.0];
                    let suspend_point_len_before = suspend_point_accumulator.len();
                    let batch_opt =
                        branch.batch_continue(&context, interrupt, tabled_functions, suspend_point_accumulator)?;
                    if suspend_point_accumulator.len() != suspend_point_len_before {
                        suspend_point_accumulator.push(SuspendPoint::new_nested(index, branch_index, input.clone()))
                    }
                    if let Some(unmapped) = batch_opt {
                        let mapped = disjunction.map_output(unmapped);
                        control_stack.push(ControlInstruction::ExecuteDisjunction(ExecuteDisjunction {
                            index,
                            branch_index,
                            input,
                        }));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), mapped)?;
                    }
                }

                ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction {
                    index,
                    parameters_override,
                    input,
                }) => {
                    let NestedPatternExecutor::InlinedFunction(executor) = &mut executors[index.0].unwrap_nested()
                    else {
                        unreachable!();
                    };
                    let inner = &mut executor.inner;
                    let suspend_point_len_before = suspend_point_accumulator.len();
                    let nested_context = ExecutionContext::new(
                        context.snapshot.clone(),
                        context.thing_manager.clone(),
                        parameters_override.clone(),
                    );
                    let unmapped_opt = inner.batch_continue(
                        &nested_context,
                        interrupt,
                        tabled_functions,
                        suspend_point_accumulator,
                    )?;
                    if suspend_point_accumulator.len() != suspend_point_len_before {
                        suspend_point_accumulator.push(SuspendPoint::new_nested(index, BranchIndex(0), input.clone()))
                    }
                    if let Some(unmapped) = unmapped_opt {
                        let mapped = executor.map_output(input.as_reference(), unmapped);
                        control_stack.push(ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction {
                            index,
                            input,
                            parameters_override,
                        }));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), mapped)?;
                    }
                }
                ControlInstruction::ExecuteNested(ExecuteNested { index, mut mapper, input, parameters_override }) => {
                    // TODO: This bit desperately needs a cleanup.
                    let inner = &mut executors[index.0].unwrap_nested().get_inner();
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
                    let unmapped = inner.batch_continue(
                        &nested_context,
                        interrupt,
                        tabled_functions,
                        suspend_point_accumulator,
                    )?;
                    if suspend_point_accumulator.len() != suspend_point_len_before {
                        suspend_point_accumulator.push(SuspendPoint::new_nested(index, BranchIndex(0), input.clone()))
                    }
                    let (must_retry, mapped) = mapper.map_output(unmapped).into_parts();
                    if must_retry {
                        control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                            index,
                            mapper,
                            input,
                            parameters_override,
                        }));
                    } else {
                        inner.reset();
                    }
                    if let Some(batch) = mapped {
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ExecuteTabledCall(TabledCall { index }) => {
                    self.execute_tabled_call(context, interrupt, tabled_functions, suspend_point_accumulator, index)?;
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
                            self.control_stack
                                .push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }))
                        }
                    }
                }
                ControlInstruction::StreamCollected(StreamCollected { index, mut iterator }) => {
                    if let Some(batch) = iterator.batch_continue()? {
                        self.control_stack
                            .push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }));
                        self.prepare_next_instruction_and_push_to_stack(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ReshapeForReturn(ReshapeForReturn { index, to_reshape: batch }) => {
                    let return_positions = executors[index.0].unwrap_reshape();
                    let mut output_batch = FixedBatch::new(return_positions.len() as u32);
                    for row in batch {
                        output_batch.append(|mut write_to| {
                            write_to.copy_mapped(
                                row,
                                return_positions
                                    .iter()
                                    .enumerate()
                                    .map(|(dst, src)| (src.clone(), VariablePosition::new(dst as u32))),
                            );
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
                    self.control_stack
                        .push(ControlInstruction::ExecuteImmediate(ExecuteImmediate { index: next_executor_index }));
                }
                StepExecutors::Nested(_) => {
                    self.control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested {
                        index: next_executor_index,
                        iterator: FixedBatchRowIterator::new(Ok(batch)),
                    }))
                }
                StepExecutors::TabledCall(_) => {
                    self.control_stack.push(ControlInstruction::MapRowBatchToRowForNested(MapRowBatchToRowForNested {
                        index: next_executor_index,
                        iterator: FixedBatchRowIterator::new(Ok(batch)),
                    }));
                }
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    self.control_stack
                        .push(ControlInstruction::CollectingStage(CollectingStage { index: next_executor_index }));
                }
                StepExecutors::ReshapeForReturn(_) => {
                    self.control_stack.push(ControlInstruction::ReshapeForReturn(ReshapeForReturn {
                        index: next_executor_index,
                        to_reshape: batch,
                    }));
                }
            }
        }
        Ok(())
    }

    fn prepare_and_push_nested_pattern(&mut self, index: ExecutorIndex, input: MaybeOwnedRow<'_>) {
        if let StepExecutors::TabledCall(tabled_call) = &mut self.executors[index.0] {
            tabled_call.prepare(input.clone().into_owned());
            self.control_stack.push(ControlInstruction::ExecuteTabledCall(TabledCall { index }));
        } else if let StepExecutors::Nested(nested) = &mut self.executors[index.0] {
            match nested {
                NestedPatternExecutor::Disjunction(Disjunction { branches, .. }) => {
                    for (branch_index, branch) in branches.iter_mut().enumerate() {
                        branch.prepare(FixedBatch::from(input.as_reference()));
                        self.control_stack.push(ControlInstruction::ExecuteDisjunction(ExecuteDisjunction {
                            index,
                            branch_index: BranchIndex(branch_index),
                            input: input.clone().into_owned(),
                        }))
                    }
                }
                NestedPatternExecutor::Negation(Negation { inner }) => {
                    inner.prepare(FixedBatch::from(input.as_reference()));
                    self.control_stack.push(ControlInstruction::ExecuteNegation(ExecuteNegation {
                        index,
                        input: input.clone().into_owned(),
                    }));
                }
                NestedPatternExecutor::InlinedFunction(InlinedFunction {
                    inner,
                    arg_mapping,
                    parameter_registry,
                    ..
                }) => {
                    let mapped_input = MaybeOwnedRow::new_owned(
                        arg_mapping.iter().map(|arg_pos| input.get(arg_pos.clone()).clone().into_owned()).collect(),
                        input.multiplicity(),
                    );
                    inner.prepare(FixedBatch::from(mapped_input));
                    self.control_stack.push(ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction {
                        index,
                        input: input.clone().into_owned(),
                        parameters_override: parameter_registry.clone(),
                    }));
                }
                NestedPatternExecutor::Offset { inner, offset } => {
                    let mut mapper = NestedPatternResultMapper::Offset(OffsetMapper::new(*offset));
                    let mapped_input = mapper.map_input(&input);
                    inner.prepare(FixedBatch::from(mapped_input));
                    self.control_stack.push(ControlInstruction::ExecuteNested(ExecuteNested {
                        index,
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
                        mapper,
                        input: input.clone().into_owned(),
                        parameters_override: None,
                    }));
                }
            }
        } else {
            unreachable!();
        }
    }

    fn execute_tabled_call(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        tabled_functions: &mut TabledFunctions,
        suspend_point_accumulator: &mut Vec<SuspendPoint>,
        index: ExecutorIndex,
    ) -> Result<(), ReadExecutionError> {
        let executor = self.executors[index.0].unwrap_tabled_call();
        let call_key = executor.active_call_key().unwrap();
        let function_state = tabled_functions.get_or_create_function_state(&context, call_key)?;
        let found = match executor.try_read_next_batch(&function_state) {
            TabledCallResult::RetrievedFromTable(batch) => Some(batch),
            TabledCallResult::Suspend => {
                suspend_point_accumulator.push(executor.create_suspend_point_for(index));
                None
            }
            TabledCallResult::MustExecutePattern(mut pattern_state_mutex_guard) => {
                let TabledFunctionPatternExecutorState { pattern_executor, suspend_points, parameters } =
                    pattern_state_mutex_guard.deref_mut();
                let context_with_function_parameters =
                    ExecutionContext::new(context.snapshot.clone(), context.thing_manager.clone(), parameters.clone());
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
            self.control_stack.push(ControlInstruction::ExecuteTabledCall(TabledCall { index: index.clone() }));
            let mapped = executor.map_output(batch);
            self.prepare_next_instruction_and_push_to_stack(context, index.next(), mapped)?;
        } else {
            // TODO: This looks like the place to consider a retry?
        }
        Ok(())
    }
}
