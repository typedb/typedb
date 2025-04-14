/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::DerefMut;

use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        control_instruction::{
            CollectingStage, ControlInstruction, ExecuteDisjunctionBranch, ExecuteImmediate, ExecuteInlinedFunction,
            ExecuteNegation, ExecuteStreamModifier, ExecuteTabledCall, MapBatchToRowsForNested, PatternStart,
            ReshapeForReturn, RestoreSuspension, StreamCollected, Yield,
        },
        nested_pattern_executor::{DisjunctionExecutor, InlinedCallExecutor, NegationExecutor},
        step_executor::StepExecutors,
        suspension::{NestedPatternSuspension, PatternSuspension, QueryPatternSuspensions, TabledCallSuspension},
        tabled_call_executor::TabledCallResult,
        tabled_functions::{TabledFunctionPatternExecutorState, TabledFunctions},
        BranchIndex, ExecutorIndex,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt, Provenance,
};

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
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let mut suspensions = QueryPatternSuspensions::new_root();
        let result = self.batch_continue(context, interrupt, tabled_functions, &mut suspensions)?;
        debug_assert!(suspensions.is_empty());
        Ok(result)
    }

    pub(crate) fn prepare(&mut self, input_batch: FixedBatch) {
        debug_assert!(self.control_stack.is_empty());
        self.reset();
        for executor in &mut self.executors {
            match executor {
                StepExecutors::Immediate(inner) => inner.reset(),
                StepExecutors::Negation(inner) => inner.reset(),
                StepExecutors::Disjunction(inner) => inner.reset(),
                StepExecutors::InlinedCall(inner) => inner.reset(),
                StepExecutors::StreamModifier(inner) => inner.reset(),
                StepExecutors::CollectingStage(inner) => inner.reset(),
                StepExecutors::TabledCall(_) | StepExecutors::ReshapeForReturn(_) => {}
            }
        }
        self.control_stack.push(PatternStart { input_batch }.into());
    }

    pub(crate) fn prepare_to_restore_from_suspension(&mut self, depth: usize) {
        debug_assert!(self.control_stack.is_empty());
        self.reset();
        self.control_stack.push(RestoreSuspension { depth }.into());
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
                        control_stack.push(RestoreSuspension { depth }.into());
                        restore_suspension(control_stack, executors, point);
                    }
                }
                ControlInstruction::ExecuteImmediate(ExecuteImmediate { index }) => {
                    let executor = executors[*index].unwrap_immediate();
                    if let Some(batch) = executor.batch_continue(context, interrupt)? {
                        control_stack.push(ExecuteImmediate { index }.into());
                        self.push_next_instruction(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::MapBatchToRowsForNested(MapBatchToRowsForNested { index, mut iterator }) => {
                    if let Some(row_result) = iterator.next() {
                        let row_owned = row_result.unwrap().into_owned();
                        control_stack.push(MapBatchToRowsForNested { index, iterator }.into());
                        self.push_nested_pattern(index, row_owned);
                    }
                }
                ControlInstruction::ExecuteNegation(ExecuteNegation { index, input }) => {
                    let NegationExecutor { inner } = &mut executors[*index].unwrap_negation();
                    let result = inner.compute_next_batch(context, interrupt, tabled_functions)?;
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
                ControlInstruction::ExecuteDisjunctionBranch(ExecuteDisjunctionBranch {
                    index,
                    branch_index,
                    input,
                }) => {
                    let disjunction = &mut executors[*index].unwrap_disjunction();
                    let branch = &mut disjunction.branches[*branch_index];
                    let batch_opt = may_push_nested(suspensions, index, branch_index, &input, |suspensions| {
                        branch.batch_continue(context, interrupt, tabled_functions, suspensions)
                    })?;
                    if let Some(mapped) = batch_opt.map(|unmapped| disjunction.map_output(branch_index, unmapped)) {
                        control_stack.push(ExecuteDisjunctionBranch { index, branch_index, input }.into());
                        self.push_next_instruction(context, index.next(), mapped)?;
                    }
                }
                ControlInstruction::ExecuteInlinedFunction(ExecuteInlinedFunction { index, input }) => {
                    let executor = &mut executors[*index].unwrap_inlined_call();
                    let func_context = &context.clone_with_replaced_parameters(executor.parameter_registry.clone());
                    let batch_opt = may_push_nested(suspensions, index, BranchIndex(0), &input, |suspensions| {
                        executor.inner.batch_continue(func_context, interrupt, tabled_functions, suspensions)
                    })?;
                    if let Some(mapped) = batch_opt.map(|batch| executor.map_output(input.as_reference(), batch)) {
                        control_stack.push(ExecuteInlinedFunction { index, input: input.into_owned() }.into());
                        self.push_next_instruction(context, index.next(), mapped)?;
                    }
                }
                ControlInstruction::ExecuteStreamModifier(ExecuteStreamModifier { index, mut mapper, input }) => {
                    let inner = &mut executors[*index].unwrap_stream_modifier().inner();
                    let unmapped = may_push_nested(suspensions, index, BranchIndex(0), &input, |suspensions| {
                        inner.batch_continue(context, interrupt, tabled_functions, suspensions)
                    })?;
                    if let Some(batch) = mapper.map_output(unmapped) {
                        control_stack.push(ExecuteStreamModifier { index, mapper, input: input.into_owned() }.into());
                        self.push_next_instruction(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ExecuteTabledCall(ExecuteTabledCall { index, last_seen_table_size }) => {
                    self.execute_tabled_call(
                        context,
                        interrupt,
                        tabled_functions,
                        suspensions,
                        index,
                        last_seen_table_size,
                    )?;
                }
                ControlInstruction::CollectingStage(CollectingStage { index, mut collector }) => {
                    let inner = executors[*index].unwrap_collecting_stage().pattern_mut();
                    while let Some(batch) = inner.compute_next_batch(context, interrupt, tabled_functions)? {
                        collector.accept(context, batch);
                    }
                    let iterator = collector.into_iterator(context);
                    self.control_stack.push(StreamCollected { index, iterator }.into());
                }
                ControlInstruction::StreamCollected(StreamCollected { index, mut iterator }) => {
                    if let Some(batch) = iterator.batch_continue()? {
                        self.control_stack.push(StreamCollected { index, iterator }.into());
                        self.push_next_instruction(context, index.next(), batch)?;
                    }
                }
                ControlInstruction::ReshapeForReturn(ReshapeForReturn { index, to_reshape: batch }) => {
                    let reshape = executors[*index].unwrap_reshape();
                    let output_batch = reshape.map_output(batch);
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
        next_index: ExecutorIndex,
        batch: FixedBatch,
    ) -> Result<(), ReadExecutionError> {
        if batch.is_empty() {
            return Ok(());
        }
        if *next_index >= self.executors.len() {
            self.control_stack.push(ControlInstruction::Yield(Yield { batch }));
        } else {
            match &mut self.executors[*next_index] {
                StepExecutors::Immediate(executable) => {
                    executable.prepare(batch, context)?;
                    self.control_stack.push(ExecuteImmediate { index: next_index }.into());
                }
                StepExecutors::Negation(_)
                | StepExecutors::Disjunction(_)
                | StepExecutors::InlinedCall(_)
                | StepExecutors::StreamModifier(_)
                | StepExecutors::TabledCall(_) => {
                    let iterator = FixedBatchRowIterator::new(Ok(batch));
                    self.control_stack.push(MapBatchToRowsForNested { index: next_index, iterator }.into())
                }
                StepExecutors::CollectingStage(collecting_stage) => {
                    collecting_stage.prepare(batch);
                    let collector = collecting_stage.create_collector();
                    self.control_stack.push(CollectingStage { index: next_index, collector }.into());
                }
                StepExecutors::ReshapeForReturn(_) => {
                    self.control_stack.push(ReshapeForReturn { index: next_index, to_reshape: batch }.into());
                }
            }
        }
        Ok(())
    }

    fn push_nested_pattern(&mut self, index: ExecutorIndex, input: MaybeOwnedRow<'_>) {
        match &mut self.executors[*index] {
            StepExecutors::TabledCall(tabled_call) => {
                tabled_call.prepare(input.clone().into_owned());
                self.control_stack.push(ExecuteTabledCall { index, last_seen_table_size: None }.into());
            }
            StepExecutors::Disjunction(DisjunctionExecutor { branches, .. }) => {
                for (idx, branch) in branches.iter_mut().enumerate() {
                    let branch_index = BranchIndex(idx);
                    branch.prepare(FixedBatch::from(input.as_reference()));
                    self.control_stack.push(
                        ExecuteDisjunctionBranch { index, branch_index, input: input.clone().into_owned() }.into(),
                    )
                }
            }
            StepExecutors::Negation(NegationExecutor { inner }) => {
                inner.prepare(FixedBatch::from(input.as_reference()));
                self.control_stack.push(ExecuteNegation { index, input: input.into_owned() }.into());
            }
            StepExecutors::InlinedCall(InlinedCallExecutor { inner, arg_mapping, .. }) => {
                let mapped_input = MaybeOwnedRow::new_owned(
                    arg_mapping.iter().map(|&arg_pos| input.get(arg_pos).clone().into_owned()).collect(),
                    input.multiplicity(),
                    Provenance::INITIAL,
                );
                inner.prepare(FixedBatch::from(mapped_input));
                self.control_stack.push(ExecuteInlinedFunction { index, input: input.into_owned() }.into());
            }
            StepExecutors::StreamModifier(stream_modifier) => {
                stream_modifier.inner().prepare(FixedBatch::from(input.as_reference()));
                let mapper = stream_modifier.create_mapper();
                self.control_stack.push(ExecuteStreamModifier { index, mapper, input: input.into_owned() }.into())
            }
            _ => unreachable!("Not called on any other StepExecutor"),
        }
    }

    fn execute_tabled_call(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        tabled_functions: &mut TabledFunctions,
        caller_suspensions: &mut QueryPatternSuspensions,
        index: ExecutorIndex,
        table_size_at_last_restore: Option<usize>,
    ) -> Result<(), ReadExecutionError> {
        let executor = self.executors[*index].unwrap_tabled_call();
        let call_key = executor.active_call_key().unwrap();
        let function_state = tabled_functions.get_or_create_function_state(context, call_key)?;
        let found = match executor.try_read_next_batch(&function_state) {
            TabledCallResult::RetrievedFromTable(batch) => Some(batch),
            TabledCallResult::Suspend => {
                caller_suspensions.push_tabled_call(index, executor);
                None
            }
            TabledCallResult::MustExecutePattern(mut pattern_state_mutex_guard) => {
                let TabledFunctionPatternExecutorState {
                    pattern_executor,
                    suspensions: function_suspensions,
                    parameters,
                } = pattern_state_mutex_guard.deref_mut();
                let batch_opt = pattern_executor.batch_continue(
                    &context.clone_with_replaced_parameters(parameters.clone()),
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
                        if function_suspensions.scc() != caller_suspensions.scc() {
                            // This was an entry into a new SCC. We might have to retry!
                            drop(pattern_state_mutex_guard);
                            let new_table_size = tabled_functions.total_table_size();
                            if Some(new_table_size) != table_size_at_last_restore {
                                tabled_functions.may_prepare_to_retry_suspended();
                                self.control_stack.push(
                                    ExecuteTabledCall { index, last_seen_table_size: Some(new_table_size) }.into(),
                                );
                            } // else, we're done!!!
                        } else {
                            caller_suspensions.push_tabled_call(index, executor);
                        }
                    }
                    None
                }
            }
        };
        if let Some(batch) = found {
            self.control_stack
                .push(ExecuteTabledCall { index, last_seen_table_size: table_size_at_last_restore }.into());
            let mapped = executor.map_output(batch);
            self.push_next_instruction(context, index.next(), mapped)?;
        }
        Ok(())
    }
}

fn restore_suspension(
    control_stack: &mut Vec<ControlInstruction>,
    executors: &mut [StepExecutors],
    point: PatternSuspension,
) {
    match point {
        PatternSuspension::AtTabledCall(suspended_call) => {
            let TabledCallSuspension { executor_index, next_table_row, input_row, .. } = suspended_call;
            let executor = executors[*executor_index].unwrap_tabled_call();
            executor.restore_from_suspension(input_row, next_table_row);
            // last_seen_table_size is None because a suspension is within a cycle, not at the entry. last_seen_table_size is set only for entry
            control_stack.push(ExecuteTabledCall { index: executor_index, last_seen_table_size: None }.into())
        }
        PatternSuspension::AtNestedPattern(suspended_nested) => {
            let NestedPatternSuspension { executor_index: index, input_row, branch_index, depth } = suspended_nested;
            let nested_pattern_depth = depth + 1;
            match &mut executors[*index] {
                StepExecutors::Negation(_) => {
                    unreachable!("Stratification must have been violated")
                }
                StepExecutors::Disjunction(disjunction) => {
                    disjunction.branches[*branch_index].prepare_to_restore_from_suspension(nested_pattern_depth);
                    control_stack
                        .push(ExecuteDisjunctionBranch { index, branch_index, input: input_row.into_owned() }.into())
                }
                StepExecutors::InlinedCall(inlined) => {
                    inlined.inner.prepare_to_restore_from_suspension(nested_pattern_depth);
                    control_stack.push(ExecuteInlinedFunction { index, input: input_row.into_owned() }.into())
                }
                StepExecutors::StreamModifier(modifier) => {
                    modifier.inner().prepare_to_restore_from_suspension(nested_pattern_depth);
                    let mapper = modifier.create_mapper();
                    control_stack.push(ExecuteStreamModifier { index, mapper, input: input_row.into_owned() }.into())
                }
                StepExecutors::Immediate(_)
                | StepExecutors::CollectingStage(_)
                | StepExecutors::TabledCall(_)
                | StepExecutors::ReshapeForReturn(_) => unreachable!("Illegal for AtPattern suspension"),
            }
        }
    }
}

#[inline]
pub(super) fn may_push_nested<Result>(
    suspensions: &mut QueryPatternSuspensions,
    executor_index: ExecutorIndex,
    branch_index: BranchIndex,
    input_row: &MaybeOwnedRow<'_>,
    nested_pattern_execution: impl FnOnce(&mut QueryPatternSuspensions) -> Result,
) -> Result {
    let suspension_count_before = suspensions.record_nested_pattern_entry();
    let result = nested_pattern_execution(suspensions);
    if suspensions.record_nested_pattern_exit() != suspension_count_before {
        suspensions.push_nested(executor_index, branch_index, input_row.clone().into_owned());
    }
    result
}
