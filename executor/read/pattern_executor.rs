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
            ExecuteNegation, ExecuteStreamModifier, MapRowBatchToRowForNested, PatternStart, ReshapeForReturn,
            RestoreSuspension, StreamCollected, TabledCall, Yield,
        },
        nested_pattern_executor::{Disjunction, InlinedFunction, Negation, NestedPatternExecutor},
        step_executor::StepExecutors,
        tabled_call_executor::TabledCallResult,
        tabled_functions::{TabledFunctionPatternExecutorState, TabledFunctions},
        NestedPatternSuspension, PatternSuspension, QueryPatternSuspensions, TabledCallSuspension,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

#[derive(Debug, Copy, Clone)]
pub(crate) struct BranchIndex(pub usize);

#[derive(Debug, Copy, Clone)]
pub(crate) struct ExecutorIndex(pub usize);

impl ExecutorIndex {
    fn next(&self) -> ExecutorIndex {
        ExecutorIndex(self.0 + 1)
    }
}

#[derive(Debug)]
pub(crate) struct PatternExecutor {
    executable_id: u64,
    executors: Vec<StepExecutors>,
    control_stack: Vec<ControlInstruction>,
}

impl PatternExecutor {
    pub fn new(executable_id: u64, executors: Vec<StepExecutors>) -> Self {
        PatternExecutor { executable_id, executors, control_stack: Vec::new() }
    }

    pub(crate) fn has_empty_control_stack(&self) -> bool {
        self.control_stack.is_empty()
    }

    pub(crate) fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        tabled_functions: &mut TabledFunctions,
        suspensions: &mut QueryPatternSuspensions,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let result = self.batch_continue(context, interrupt, tabled_functions, suspensions)?;
        debug_assert!(suspensions.current_depth == 0);
        Ok(result)
    }

    pub(crate) fn prepare(&mut self, input_batch: FixedBatch) {
        debug_assert!(self.control_stack.is_empty());
        self.reset();
        for executor in &mut self.executors {
            match executor {
                StepExecutors::Nested(inner) => inner.reset(),
                StepExecutors::StreamModifier(inner) => inner.reset(),
                StepExecutors::CollectingStage(inner) => inner.reset(),
                StepExecutors::Immediate(inner) => inner.reset(),
                StepExecutors::TabledCall(_) | StepExecutors::ReshapeForReturn(_) => {}
            }
        }
        self.control_stack.push(ControlInstruction::PatternStart(PatternStart { input_batch }));
    }

    pub(crate) fn prepare_to_restore_from_suspension(&mut self, depth: usize) {
        debug_assert!(self.control_stack.is_empty());
        self.reset();
        self.control_stack.push(ControlInstruction::RestoreSuspension(RestoreSuspension { depth }));
    }

    pub(crate) fn reset(&mut self) {
        self.control_stack.clear();
    }

    pub(super) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        tabled_functions: &mut TabledFunctions,
        suspensions: &mut QueryPatternSuspensions,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        // TODO: In debug mode, this function has a frame of ~60k, causing an overflow at ~10 frames
        //  In release mode, the frame is ~10x smaller, allowing ~100 frames.
        //  We could switch to iteration & handle the stack ourselves: StackFrame { pattern_executor, return_address }
        while self.control_stack.last().is_some() {
            let Self { control_stack, executors, executable_id: _ } = self;
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            match control_stack.pop().unwrap() {
                ControlInstruction::PatternStart(PatternStart { input_batch }) => {
                    self.push_next_instruction(context, ExecutorIndex(0), input_batch)?;
                }
                ControlInstruction::RestoreSuspension(RestoreSuspension { depth }) => {
                    debug_assert!(depth == suspensions.current_depth()); // Smell. The depth in the step is redundant
                    if let Some(point) = suspensions.next_restore_point_at_current_depth() {
                        restore_suspension(control_stack, executors, depth, point);
                    }
                }
                ControlInstruction::ExecuteImmediate(ExecuteImmediate { index }) => {
                    let executor = executors[index.0].unwrap_immediate();
                    if let Some(batch) = executor.batch_continue(context, interrupt)? {
                        control_stack.push(ControlInstruction::ExecuteImmediate(ExecuteImmediate { index }));
                        self.push_next_instruction(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::MapBatchToRowForNested(mut batch_mapper) => {
                    if let Some(row_result) = batch_mapper.iterator.next() {
                        let index = batch_mapper.index;
                        let unmapped_input = row_result.unwrap().into_owned();
                        control_stack.push(ControlInstruction::MapBatchToRowForNested(batch_mapper));
                        self.push_nested_pattern(index, unmapped_input);
                    }
                }
                ControlInstruction::ExecuteNegation(ExecuteNegation { index, input }) => {
                    let StepExecutors::Nested(NestedPatternExecutor::Negation(Negation { inner })) =
                        &mut executors[index.0]
                    else {
                        unreachable!();
                    };
                    let mut negation_suspensions = QueryPatternSuspensions::new_root();
                    let result =
                        inner.compute_next_batch(context, interrupt, tabled_functions, &mut negation_suspensions)?;
                    debug_assert!(negation_suspensions.is_empty());
                    match result {
                        None => {
                            self.push_next_instruction(context, index.next(), FixedBatch::from(input.as_reference()))?
                        }
                        Some(batch) => {
                            debug_assert!(!batch.is_empty());
                            inner.reset()
                        }
                    };
                }
                ControlInstruction::ExecuteDisjunction(ExecuteDisjunction { index, branch_index, input }) => {
                    let NestedPatternExecutor::Disjunction(disjunction) = &mut executors[index.0].unwrap_nested()
                    else {
                        unreachable!();
                    };
                    let branch = &mut disjunction.branches[branch_index.0];
                    let suspension_count_before = suspensions.record_nested_pattern_entry();
                    let batch_opt = branch.batch_continue(context, interrupt, tabled_functions, suspensions)?;
                    if suspensions.record_nested_pattern_exit() != suspension_count_before {
                        suspensions.push_nested(index, branch_index, input.clone());
                    }
                    if let Some(unmapped) = batch_opt {
                        let mapped = disjunction.map_output(unmapped);
                        control_stack.push(ControlInstruction::ExecuteDisjunction(ExecuteDisjunction {
                            index,
                            branch_index,
                            input,
                        }));
                        self.push_next_instruction(context, index.next(), mapped)?;
                    }
                }
                ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction { index, input }) => {
                    let NestedPatternExecutor::InlinedFunction(executor) = &mut executors[index.0].unwrap_nested()
                    else {
                        unreachable!();
                    };
                    let suspension_count_before = suspensions.record_nested_pattern_entry();
                    let unmapped_opt = executor.inner.batch_continue(
                        &context.clone_with_replaced_parameters(executor.parameter_registry.clone()),
                        interrupt,
                        tabled_functions,
                        suspensions,
                    )?;
                    if suspensions.record_nested_pattern_exit() != suspension_count_before {
                        suspensions.push_nested(index, BranchIndex(0), input.clone());
                    }
                    if let Some(unmapped) = unmapped_opt {
                        let mapped = executor.map_output(input.as_reference(), unmapped);
                        control_stack
                            .push(ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction { index, input }));
                        self.push_next_instruction(context, index.next(), mapped)?;
                    }
                }
                ControlInstruction::ExecuteStreamModifier(ExecuteStreamModifier { index, mut mapper, input }) => {
                    let inner = &mut executors[index.0].unwrap_stream_modifier().inner();
                    let suspension_count_before = suspensions.record_nested_pattern_entry();
                    let unmapped = inner.batch_continue(context, interrupt, tabled_functions, suspensions)?;
                    if suspensions.record_nested_pattern_exit() != suspension_count_before {
                        suspensions.push_nested(index, BranchIndex(0), input.clone());
                    }
                    let (must_retry, mapped) = mapper.map_output(unmapped).into_parts();
                    if must_retry {
                        control_stack.push(ControlInstruction::ExecuteStreamModifier(ExecuteStreamModifier {
                            index,
                            mapper,
                            input,
                        }));
                    } else {
                        inner.reset();
                    }
                    if let Some(batch) = mapped {
                        self.push_next_instruction(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ExecuteTabledCall(TabledCall { index, last_seen_table_size }) => {
                    self.execute_tabled_call(
                        context,
                        interrupt,
                        tabled_functions,
                        suspensions,
                        index,
                        last_seen_table_size,
                    )?;
                }
                ControlInstruction::CollectingStage(CollectingStage { index }) => {
                    let (inner, collector) = executors[index.0].unwrap_collecting_stage().to_parts_mut();
                    let mut inner_suspensions = QueryPatternSuspensions::new_root();
                    while let Some(batch) =
                        inner.compute_next_batch(context, interrupt, tabled_functions, &mut inner_suspensions)?
                    {
                        collector.accept(context, batch);
                    }
                    debug_assert!(inner_suspensions.is_empty());
                    let iterator = collector.collected_to_iterator(context);
                    self.control_stack.push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }));
                }
                ControlInstruction::StreamCollected(StreamCollected { index, mut iterator }) => {
                    if let Some(batch) = iterator.batch_continue()? {
                        self.control_stack
                            .push(ControlInstruction::StreamCollected(StreamCollected { index, iterator }));
                        self.push_next_instruction(context, index.next(), batch)?;
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
                                    .map(|(dst, &src)| (src, VariablePosition::new(dst as u32))),
                            );
                        })
                    }
                    self.push_next_instruction(context, index.next(), output_batch)?;
                }
                ControlInstruction::Yield(Yield { batch }) => {
                    return Ok(Some(batch));
                }
            }
        }
        Ok(None) // Nothing in the stack
    }

    fn push_next_instruction(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        next_executor_index: ExecutorIndex,
        batch: FixedBatch,
    ) -> Result<(), ReadExecutionError> {
        if batch.is_empty() {
            return Ok(());
        }
        if next_executor_index.0 >= self.executors.len() {
            self.control_stack.push(ControlInstruction::Yield(Yield { batch }));
        } else {
            match &mut self.executors[next_executor_index.0] {
                StepExecutors::Immediate(executable) => {
                    executable.prepare(batch, context)?;
                    self.control_stack
                        .push(ControlInstruction::ExecuteImmediate(ExecuteImmediate { index: next_executor_index }));
                }
                StepExecutors::Nested(_) | StepExecutors::StreamModifier(_) | StepExecutors::TabledCall(_) => {
                    self.control_stack.push(ControlInstruction::MapBatchToRowForNested(MapRowBatchToRowForNested {
                        index: next_executor_index,
                        iterator: FixedBatchRowIterator::new(Ok(batch)),
                    }))
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

    fn push_nested_pattern(&mut self, index: ExecutorIndex, input: MaybeOwnedRow<'_>) {
        if let StepExecutors::TabledCall(tabled_call) = &mut self.executors[index.0] {
            tabled_call.prepare(input.clone().into_owned());
            self.control_stack
                .push(ControlInstruction::ExecuteTabledCall(TabledCall { index, last_seen_table_size: None }));
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
                NestedPatternExecutor::InlinedFunction(InlinedFunction { inner, arg_mapping, .. }) => {
                    let mapped_input = MaybeOwnedRow::new_owned(
                        arg_mapping.iter().map(|&arg_pos| input.get(arg_pos).clone().into_owned()).collect(),
                        input.multiplicity(),
                    );
                    inner.prepare(FixedBatch::from(mapped_input));
                    self.control_stack.push(ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction {
                        index,
                        input: input.clone().into_owned(),
                    }));
                }
            }
        } else if let StepExecutors::StreamModifier(stream_modifier) = &mut self.executors[index.0] {
            stream_modifier.inner().prepare(FixedBatch::from(input.as_reference()));
            let mapper = stream_modifier.create_mapper();
            self.control_stack.push(ControlInstruction::ExecuteStreamModifier(ExecuteStreamModifier {
                index,
                mapper,
                input: input.clone().into_owned(),
            }));
        } else {
            unreachable!();
        }
    }

    fn execute_tabled_call(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        tabled_functions: &mut TabledFunctions,
        query_suspensions: &mut QueryPatternSuspensions,
        index: ExecutorIndex,
        table_size_at_last_restore: Option<usize>,
    ) -> Result<(), ReadExecutionError> {
        let executor = self.executors[index.0].unwrap_tabled_call();
        let call_key = executor.active_call_key().unwrap();
        let function_state = tabled_functions.get_or_create_function_state(context, call_key)?;
        let found = match executor.try_read_next_batch(&function_state) {
            TabledCallResult::RetrievedFromTable(batch) => Some(batch),
            TabledCallResult::Suspend => {
                query_suspensions.push_tabled_call(index, executor);
                None
            }
            TabledCallResult::MustExecutePattern(mut pattern_state_mutex_guard) => {
                let TabledFunctionPatternExecutorState {
                    pattern_executor,
                    suspensions: function_suspensions,
                    parameters,
                } = pattern_state_mutex_guard.deref_mut();
                let context_with_function_parameters =
                    ExecutionContext::new(context.snapshot.clone(), context.thing_manager.clone(), parameters.clone());
                let batch_opt = pattern_executor.batch_continue(
                    &context_with_function_parameters,
                    interrupt,
                    tabled_functions,
                    function_suspensions,
                )?;
                if let Some(batch) = batch_opt {
                    let deduplicated_batch = executor.add_batch_to_table(&function_state, batch);
                    Some(deduplicated_batch)
                } else {
                    // Don't use suspend_count_before == suspend_count_after, since we can get away with just one.
                    if !function_suspensions.is_empty() {
                        // TODO: Consider retrying here. For now, just record a suspension point for ourselves.
                        if function_suspensions.scc() != query_suspensions.scc() {
                            // This was an entry into a new SCC. We might have to retry!
                            drop(pattern_state_mutex_guard);
                            let new_table_size = tabled_functions.total_table_size();
                            if Some(new_table_size) != table_size_at_last_restore {
                                tabled_functions.may_prepare_to_retry_suspended();
                                self.control_stack.push(ControlInstruction::ExecuteTabledCall(TabledCall {
                                    index,
                                    last_seen_table_size: Some(new_table_size),
                                }));
                            } // else, we're done!!!
                        } else {
                            query_suspensions.push_tabled_call(index, executor);
                        }
                    }
                    None
                }
            }
        };
        if let Some(batch) = found {
            self.control_stack.push(ControlInstruction::ExecuteTabledCall(TabledCall {
                index,
                last_seen_table_size: table_size_at_last_restore,
            }));
            let mapped = executor.map_output(batch);
            self.push_next_instruction(context, index.next(), mapped)?;
        }
        Ok(())
    }
}

fn restore_suspension(
    control_stack: &mut Vec<ControlInstruction>,
    executors: &mut [StepExecutors],
    depth: usize,
    point: PatternSuspension,
) {
    control_stack.push(ControlInstruction::RestoreSuspension(RestoreSuspension { depth }));
    match point {
        PatternSuspension::AtTabledCall(suspended_call) => {
            let TabledCallSuspension { executor_index, next_table_row, input_row, .. } = suspended_call;
            let executor = executors[executor_index.0].unwrap_tabled_call();
            executor.restore_from_suspension(input_row, next_table_row);
            // last_seen_table_size is None because a suspension is within a cycle, not at the entry. last_seen_table_size is set only for entry
            control_stack.push(ControlInstruction::ExecuteTabledCall(TabledCall {
                index: executor_index,
                last_seen_table_size: None,
            }))
        }
        PatternSuspension::AtNestedPattern(suspended_nested) => {
            let NestedPatternSuspension { executor_index, input_row, branch_index, depth } = suspended_nested;
            let nested_pattern_depth = depth + 1;
            match &mut executors[executor_index.0] {
                StepExecutors::Nested(nested) => match nested {
                    NestedPatternExecutor::Negation(_) => {
                        unreachable!("Stratification must have been violated")
                    }
                    NestedPatternExecutor::Disjunction(disjunction) => {
                        disjunction.branches[branch_index.0].prepare_to_restore_from_suspension(nested_pattern_depth);
                        control_stack.push(ControlInstruction::ExecuteDisjunction(ExecuteDisjunction {
                            index: executor_index,
                            branch_index,
                            input: input_row.into_owned(),
                        }))
                    }
                    NestedPatternExecutor::InlinedFunction(inlined) => {
                        inlined.inner.prepare_to_restore_from_suspension(nested_pattern_depth);
                        control_stack.push(ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction {
                            index: executor_index,
                            input: input_row.into_owned(),
                        }))
                    }
                },
                StepExecutors::StreamModifier(modifier) => {
                    modifier.inner().prepare_to_restore_from_suspension(nested_pattern_depth);
                    control_stack.push(ControlInstruction::ExecuteStreamModifier(ExecuteStreamModifier {
                        index: executor_index,
                        mapper: modifier.create_mapper(),
                        input: input_row.into_owned(),
                    }))
                }
                StepExecutors::Immediate(_)
                | StepExecutors::CollectingStage(_)
                | StepExecutors::TabledCall(_)
                | StepExecutors::ReshapeForReturn(_) => unreachable!("Illegal for AtPattern suspension"),
            }
        }
    }
}
