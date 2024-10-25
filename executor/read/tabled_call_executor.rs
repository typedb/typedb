/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Mutex;

use compiler::VariablePosition;
use ir::pipeline::function_signature::FunctionID;
use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    read::{
        pattern_executor::InstructionIndex,
        tabled_functions::{CallKey, TableIndex, TabledFunctionPatternExecutorState, TabledFunctionState},
        SuspendPoint, TabledCallSuspension,
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

impl TabledCallExecutor {
    pub(crate) fn add_batch_to_table(&mut self, state: &TabledFunctionState, batch: FixedBatch) -> FixedBatch {
        let deduplicated_batch = state.add_batch_to_table(batch);
        *self.active_executor.as_mut().unwrap().next_table_row += deduplicated_batch.len() as usize;
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

    pub(crate) fn read_table_or_get_executor<'a>(
        &mut self,
        tabled_function_state: &'a TabledFunctionState,
    ) -> Either<FixedBatch, &'a Mutex<TabledFunctionPatternExecutorState>> {
        // Maybe return a batch?
        let executor = self.active_executor.as_mut().unwrap();
        let read_index = &mut *executor.next_table_row;

        let table_read = tabled_function_state.table.read().unwrap();
        if *read_index < table_read.len() {
            let mut batch = FixedBatch::new(table_read.width());
            while !batch.is_full() && *read_index < table_read.len() {
                batch.append(|mut write_to| {
                    write_to.copy_from_row(table_read.get_row(*read_index).unwrap().as_reference());
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
