/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, RwLock, TryLockError, TryLockResult},
};

use clap::Parser;
use compiler::{executable::match_::planner::function_plan::ExecutableFunctionRegistry, VariablePosition};
use ir::pipeline::function_signature::FunctionID;
use itertools::Either;

use crate::{
    batch::FixedBatch, error::ReadExecutionError, read::pattern_executor::PatternExecutor, row::MaybeOwnedRow,
};

// TODO: Rearrange file

pub struct TabledFunctions {
    function_registry: Arc<ExecutableFunctionRegistry>,
    state: HashMap<CallKey, Arc<Mutex<TabledFunctionState>>>,
}

impl TabledFunctions {
    pub(crate) fn get_or_create_state_mutex(&mut self, call_key: &CallKey) -> Arc<Mutex<TabledFunctionState>> {
        if !self.state.contains_key(call_key) {
            self.state.insert(call_key.clone(), Arc::new(Mutex::new(TabledFunctionState::new())));
        }
        self.state.get(call_key).unwrap().clone()
    }
}

impl TabledFunctions {
    pub(crate) fn new(function_registry: Arc<ExecutableFunctionRegistry>) -> Self {
        Self { state: HashMap::new(), function_registry }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct CallKey {
    pub(crate) function_id: FunctionID,
    pub(crate) arguments: MaybeOwnedRow<'static>,
}

struct SuspendPoint {
    // TODO
}

pub(crate) struct TabledFunctionState {
    table: AnswerTable, // TODO: Need a structure which can de-duplicate & preserve insertion order.
    suspend_points: Vec<SuspendPoint>,
    pattern_executor: PatternExecutor,
    // can_clean: bool, // TODO: Apparently you only need to keep the table if it's a call that causes a suspend.
}

impl TabledFunctionState {
    fn new() -> Self {
        Self { table: AnswerTable { answers: Vec::new() }, suspend_points: Vec::new(), pattern_executor: todo!() }
    }
}

struct AnswerTable {
    answers: Vec<MaybeOwnedRow<'static>>,
}

impl AnswerTable {
    fn read_answer(&mut self, index: usize) -> Option<MaybeOwnedRow<'_>> {
        self.answers.get(index).map(|row| row.as_reference())
    }

    fn may_add_row(&mut self, row: MaybeOwnedRow<'static>) -> Option<usize> {
        if self.answers.contains(&row) {
            None
        } else {
            let ret = Some(self.answers.len() - 1);
            self.answers.push(row);
            ret
        }
    }
}

pub(crate) struct TabledCallExecutor {
    function_id: FunctionID,
    argument_positions: Vec<VariablePosition>,
    assignment_positions: Vec<VariablePosition>,
    active_executor: Option<TabledCallExecutorState>,
}

pub struct TabledCallExecutorState {
    pub(crate) call_key: CallKey,
    pub(crate) next_index: usize,
}

impl TabledCallExecutor {
    pub(crate) fn new(
        function_id: FunctionID,
        argument_positions: Vec<VariablePosition>,
        assignment_positions: Vec<VariablePosition>,
    ) -> Self {
        Self { function_id, argument_positions, assignment_positions, active_executor: None }
    }

    pub(crate) fn prepare(&mut self, arguments: MaybeOwnedRow<'static>) {
        let call_key = CallKey { function_id: self.function_id.clone(), arguments };
        self.active_executor = Some(TabledCallExecutorState { call_key, next_index: 0 });
    }

    pub(crate) fn active_call_key(&self) -> Option<&CallKey> {
        self.active_executor.as_ref().map(|active| &active.call_key)
    }

    pub(crate) fn batch_continue_or_function_pattern<'a>(
        &mut self,
        tabled_function_state: &'a mut TabledFunctionState,
    ) -> Either<FixedBatch, &'a mut PatternExecutor> {
        // Maybe return a batch?
        let executor = self.active_executor.as_mut().unwrap();
        if executor.next_index < tabled_function_state.table.answers.len() {
            let answer_vec = &tabled_function_state.table.answers;
            let mut batch = FixedBatch::new(answer_vec.last().unwrap().len() as u32);
            while !batch.is_full() && executor.next_index < answer_vec.len() {
                batch.append(|mut write_to| {
                    write_to.copy_from_row(answer_vec.get(executor.next_index).unwrap().as_reference());
                });
                executor.next_index += 1;
            }
            Either::Left(batch)
        } else {
            Either::Right(&mut tabled_function_state.pattern_executor)
        }
    }
}
