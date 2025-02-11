/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    fmt,
    sync::{MutexGuard, TryLockError},
};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use ir::pipeline::function_signature::FunctionID;

use crate::{
    batch::FixedBatch,
    read::{
        pattern_executor::ExecutorIndex,
        tabled_call_executor::TabledCallResult::Suspend,
        tabled_functions::{CallKey, TableIndex, TabledFunctionPatternExecutorState, TabledFunctionState},
        PatternSuspension, TabledCallSuspension,
    },
    row::MaybeOwnedRow,
};

pub(crate) struct TabledCallExecutor {
    function_id: FunctionID,
    argument_positions: Vec<VariablePosition>,
    assignment_positions: Vec<VariablePosition>,
    output_width: u32,
    active_executor: Option<TabledCallExecutorState>,
}

impl fmt::Debug for TabledCallExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TabledCallExecutor (function id {:?})", self.function_id)
    }
}

pub struct TabledCallExecutorState {
    pub(crate) call_key: CallKey,
    pub(crate) input: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

pub(super) enum TabledCallResult<'a> {
    RetrievedFromTable(FixedBatch),
    MustExecutePattern(MutexGuard<'a, TabledFunctionPatternExecutorState>),
    Suspend,
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
        self.prepare_impl(input, TableIndex(0))
    }

    pub(crate) fn restore_from_suspension(&mut self, input: MaybeOwnedRow<'static>, next_table_index: TableIndex) {
        self.prepare_impl(input, next_table_index);
    }
    pub fn prepare_impl(&mut self, input: MaybeOwnedRow<'static>, next_table_row: TableIndex) {
        let arguments = MaybeOwnedRow::new_owned(
            self.argument_positions.iter().map(|pos| input.get(*pos).to_owned()).collect(),
            input.multiplicity(),
        );
        let call_key = CallKey { function_id: self.function_id.clone(), arguments };
        self.active_executor = Some(TabledCallExecutorState { call_key, input, next_table_row });
    }

    pub(crate) fn active_call_key(&self) -> Option<&CallKey> {
        self.active_executor.as_ref().map(|active| &active.call_key)
    }

    pub(crate) fn map_output(&self, returned_batch: FixedBatch) -> FixedBatch {
        let input = &self.active_executor.as_ref().unwrap().input;
        let mut output_batch = FixedBatch::new(self.output_width);
        let check_indices: Vec<_> = self
            .assignment_positions
            .iter()
            .enumerate()
            .map(|(src, &dst)| (VariablePosition::new(src as u32), dst))
            .filter(|(src, dst)| dst.as_usize() < input.len() && input.get(*dst) != &VariableValue::Empty)
            .collect(); // TODO: Can we move this to compilation?

        for return_index in 0..returned_batch.len() {
            // TODO: Deduplicate?
            let returned_row = returned_batch.get_row(return_index);
            if check_indices.iter().all(|(src, dst)| returned_row.get(*src) == input.get(*dst)) {
                output_batch.append(|mut output_row| {
                    for (i, element) in input.iter().enumerate() {
                        output_row.set(VariablePosition::new(i as u32), element.clone());
                    }
                    for (returned_index, output_position) in self.assignment_positions.iter().enumerate() {
                        output_row.set(*output_position, returned_row[returned_index].clone().into_owned());
                    }
                });
            }
        }
        output_batch
    }

    pub(crate) fn create_suspension_at(&self, executor_index: ExecutorIndex, depth: usize) -> PatternSuspension {
        PatternSuspension::AtTabledCall(TabledCallSuspension {
            executor_index,
            depth,
            input_row: self.active_executor.as_ref().unwrap().input.clone().into_owned(),
            next_table_row: self.active_executor.as_ref().unwrap().next_table_row,
        })
    }

    pub(crate) fn try_read_next_batch<'a>(
        &mut self,
        tabled_function_state: &'a TabledFunctionState,
    ) -> TabledCallResult<'a> {
        // Maybe return a batch?
        let executor = self.active_executor.as_mut().unwrap();
        let table_read = tabled_function_state.table.read().unwrap();
        if *executor.next_table_row < table_read.len() {
            let batch = table_read.read_batch_starting(executor.next_table_row);
            *executor.next_table_row += batch.len() as usize;
            TabledCallResult::RetrievedFromTable(batch)
        } else {
            drop(table_read);
            match tabled_function_state.executor_state.try_lock() {
                Ok(executor_mutex_guard) => TabledCallResult::MustExecutePattern(executor_mutex_guard),
                Err(TryLockError::WouldBlock) => Suspend,
                Err(TryLockError::Poisoned(_)) => panic!("The mutex on a tabled function was poisoned"),
            }
        }
    }

    pub(crate) fn add_batch_to_table(&mut self, state: &TabledFunctionState, batch: FixedBatch) -> FixedBatch {
        let deduplicated_batch = state.add_batch_to_table(batch);
        *self.active_executor.as_mut().unwrap().next_table_row += deduplicated_batch.len() as usize;
        deduplicated_batch
    }
}
