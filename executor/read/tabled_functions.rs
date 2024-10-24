/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};

use compiler::{executable::match_::planner::function_plan::ExecutableFunctionRegistry, VariablePosition};
use ir::pipeline::function_signature::FunctionID;
use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        pattern_executor::{InstructionIndex, PatternExecutor},
        step_executor::create_executors_for_function,
        SuspendPoint, TabledCallSuspension,
    },
    row::MaybeOwnedRow,
};

// TODO: Rearrange file

pub struct TabledFunctions {
    function_registry: Arc<ExecutableFunctionRegistry>,
    state: HashMap<CallKey, Arc<TabledFunctionState>>,
}

impl TabledFunctions {
    pub(crate) fn get_or_create_function_state(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        call_key: &CallKey,
    ) -> Result<Arc<TabledFunctionState>, ReadExecutionError> {
        if !self.state.contains_key(call_key) {
            let function = &self.function_registry.get(call_key.function_id.clone());
            let executors = create_executors_for_function(
                &context.snapshot,
                &context.thing_manager,
                &self.function_registry,
                function,
                &mut HashSet::new(),
            )
            .map_err(|source| ReadExecutionError::ConceptRead { source })?;
            let pattern_executor = PatternExecutor::new(executors);
            self.state.insert(
                call_key.clone(),
                Arc::new(TabledFunctionState::build_and_prepare(pattern_executor, &call_key.arguments)),
            );
        }
        Ok(self.state.get(call_key).unwrap().clone())
    }
}

impl TabledFunctions {
    pub(crate) fn new(function_registry: Arc<ExecutableFunctionRegistry>) -> Self {
        Self { state: HashMap::new(), function_registry }
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) struct CallKey {
    pub(crate) function_id: FunctionID,
    pub(crate) arguments: MaybeOwnedRow<'static>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct TableIndex(usize);

pub(crate) struct TabledFunctionState {
    table: RwLock<AnswerTable>, // TODO: Need a structure which can de-duplicate & preserve insertion order.
    executor_state: Mutex<TabledFunctionPatternExecutorState>,
    // can_clean: bool, // TODO: Apparently you only need to keep the table if it's a call that causes a suspend.
}

pub(crate) struct TabledFunctionPatternExecutorState {
    pub(crate) suspend_points: Vec<SuspendPoint>,
    pub(crate) pattern_executor: PatternExecutor,
}

impl TabledFunctionState {
    fn build_and_prepare(mut pattern_executor: PatternExecutor, args: &MaybeOwnedRow<'_>) -> Self {
        pattern_executor.prepare(FixedBatch::from(args.as_reference()));
        Self {
            table: RwLock::new(AnswerTable { answers: Vec::new() }),
            executor_state: Mutex::new(TabledFunctionPatternExecutorState {
                pattern_executor,
                suspend_points: Vec::new(),
            }),
        }
    }

    fn add_batch_to_table(&self, batch: FixedBatch) -> FixedBatch {
        if !batch.is_empty() {
            let mut deduplicated_batch = FixedBatch::new(batch.get_row(0).len() as u32);
            let mut table = self.table.write().unwrap();
            let mut batch_iter = batch.into_iterator();
            while let Some(row_result) = batch_iter.next() {
                let row = row_result.unwrap();
                if table.try_add_row(row.as_reference()) {
                    deduplicated_batch.append(|mut write_to| {
                        write_to.copy_from_row(row)
                    })
                }
            }
            deduplicated_batch
        } else {
            batch
        }
    }
}

struct AnswerTable {
    answers: Vec<MaybeOwnedRow<'static>>,
}

impl AnswerTable {
    fn read_answer(&mut self, index: usize) -> Option<MaybeOwnedRow<'_>> {
        self.answers.get(index).map(|row| row.as_reference())
    }

    fn try_add_row(&mut self, row: MaybeOwnedRow<'_>) -> bool {
        if !self.answers.contains(&row) {
            self.answers.push(row.clone().into_owned());
            true
        } else {
            false
        }
    }
}

pub(crate) struct TabledCallExecutor {
    function_id: FunctionID,
    argument_positions: Vec<VariablePosition>,
    assignment_positions: Vec<VariablePosition>,
    output_width: u32,
    active_executor: Option<TabledCallExecutorState>,
}

impl TabledCallExecutor {
    pub(crate) fn add_batch_to_table(&mut self, state: &TabledFunctionState, batch: FixedBatch) -> FixedBatch {
        let deduplicated_batch = state.add_batch_to_table(batch);
        self.active_executor.as_mut().unwrap().next_table_row.0 += deduplicated_batch.len() as usize;
        deduplicated_batch
    }
}

pub struct TabledCallExecutorState {
    pub(crate) call_key: CallKey,
    pub(crate) input: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

impl TabledCallExecutor {
    pub(crate) fn new(
        function_id: FunctionID,
        argument_positions: Vec<VariablePosition>,
        assignment_positions: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self { function_id, argument_positions, assignment_positions, output_width, active_executor: None }
    }

    pub(crate) fn prepare(&mut self, input: MaybeOwnedRow<'static>) {
        let arguments = MaybeOwnedRow::new_owned(
            self.argument_positions.iter().map(|pos| input.get(pos.clone()).to_owned()).collect(),
            input.multiplicity(),
        );
        let call_key = CallKey { function_id: self.function_id.clone(), arguments };
        self.active_executor = Some(TabledCallExecutorState { call_key, input, next_table_row: TableIndex(0) });
    }

    pub(crate) fn active_call_key(&self) -> Option<&CallKey> {
        self.active_executor.as_ref().map(|active| &active.call_key)
    }

    pub(crate) fn map_output(&self, returned_batch: FixedBatch) -> FixedBatch {
        let mut output_batch = FixedBatch::new(self.output_width);
        for return_index in 0..returned_batch.len() {
            // TODO: Deduplicate?
            let returned_row = returned_batch.get_row(return_index);
            output_batch.append(|mut output_row| {
                for (i, element) in self.active_executor.as_ref().unwrap().input.iter().enumerate() {
                    output_row.set(VariablePosition::new(i as u32), element.clone());
                }
                for (returned_index, output_position) in self.assignment_positions.iter().enumerate() {
                    output_row.set(output_position.clone(), returned_row[returned_index].clone().into_owned());
                }
            });
        }
        output_batch
    }

    pub(crate) fn batch_continue_or_function_pattern<'a>(
        &mut self,
        tabled_function_state: &'a TabledFunctionState,
    ) -> Either<FixedBatch, &'a Mutex<TabledFunctionPatternExecutorState>> {
        // Maybe return a batch?
        let executor = self.active_executor.as_mut().unwrap();
        let read_index = &mut executor.next_table_row.0;

        let table_read = tabled_function_state.table.read().unwrap();
        if *read_index < table_read.answers.len() {
            let mut batch = FixedBatch::new(table_read.answers.last().unwrap().len() as u32);
            while !batch.is_full() && *read_index < table_read.answers.len() {
                batch.append(|mut write_to| {
                    write_to.copy_from_row(table_read.answers.get(*read_index).unwrap().as_reference());
                });
                *read_index += 1;
            }
            Either::Left(batch)
        } else {
            drop(table_read);
            Either::Right(&tabled_function_state.executor_state)
        }
    }

    pub(crate) fn create_suspend_point_for(&self, instruction_index: InstructionIndex) -> SuspendPoint {
        SuspendPoint::TabledCall(TabledCallSuspension {
            instruction_index,
            input_row: self.active_executor.as_ref().unwrap().input.clone().into_owned(),
            next_table_row: self.active_executor.as_ref().unwrap().next_table_row,
        })
    }
}
