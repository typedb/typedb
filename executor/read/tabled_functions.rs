/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use compiler::executable::{function::ExecutableReturn, match_::planner::function_plan::ExecutableFunctionRegistry};
use ir::pipeline::{function_signature::FunctionID, ParameterRegistry};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{pattern_executor::PatternExecutor, step_executor::create_executors_for_function, QueryPatternSuspensions},
    row::MaybeOwnedRow,
};

pub struct TabledFunctions {
    function_registry: Arc<ExecutableFunctionRegistry>,
    state: HashMap<CallKey, Arc<TabledFunctionState>>,
}

impl TabledFunctions {
    pub(crate) fn new(function_registry: Arc<ExecutableFunctionRegistry>) -> Self {
        Self { state: HashMap::new(), function_registry }
    }

    pub(crate) fn get_or_create_function_state(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        call_key: &CallKey,
    ) -> Result<Arc<TabledFunctionState>, ReadExecutionError> {
        if !self.state.contains_key(call_key) {
            let function = &self.function_registry.get(&call_key.function_id).unwrap();
            let executors = create_executors_for_function(
                &context.snapshot,
                &context.thing_manager,
                &self.function_registry,
                &context.profile,
                function,
            )
            .map_err(|source| ReadExecutionError::ConceptRead { source })?;
            let pattern_executor = PatternExecutor::new(function.executable_id, executors);
            let width = match &function.returns {
                ExecutableReturn::Stream(v) => v.len() as u32,
                _ => todo!(),
            };
            self.state.insert(
                call_key.clone(),
                Arc::new(TabledFunctionState::build_and_prepare(
                    pattern_executor,
                    &call_key.arguments,
                    width,
                    function.parameter_registry.clone(),
                )),
            );
        }
        Ok(self.state.get(call_key).unwrap().clone())
    }

    pub(crate) fn iterate_states(&self) -> impl Iterator<Item = Arc<TabledFunctionState>> + '_ {
        self.state.values().cloned()
    }

    pub(crate) fn total_table_size(&self) -> usize {
        self.state.values().map(|state| state.table.read().unwrap().answers.len()).sum()
    }
}

pub(crate) struct TabledFunctionState {
    pub(crate) table: RwLock<AnswerTable>,
    pub(crate) executor_state: Mutex<TabledFunctionPatternExecutorState>,
    // can_cleanup: bool, // TODO: Apparently you only need to keep the table if it's a call that causes a suspend.
}

pub(crate) struct TabledFunctionPatternExecutorState {
    pub(crate) suspensions: QueryPatternSuspensions,
    pub(crate) pattern_executor: PatternExecutor,
    pub(crate) parameters: Arc<ParameterRegistry>,
}

impl TabledFunctionPatternExecutorState {
    pub(crate) fn prepare_to_retry_suspended(&mut self) {
        debug_assert!(self.pattern_executor.has_empty_control_stack());
        self.pattern_executor.reset();
        if !self.suspensions.is_empty() {
            self.suspensions.prepare_restoring_from_suspending();
            self.pattern_executor.prepare_to_restore_from_suspension(0);
        }
    }
}

impl TabledFunctionState {
    fn build_and_prepare(
        mut pattern_executor: PatternExecutor,
        args: &MaybeOwnedRow<'_>,
        answer_width: u32,
        parameters: Arc<ParameterRegistry>,
    ) -> Self {
        pattern_executor.prepare(FixedBatch::from(args.as_reference()));
        Self {
            table: RwLock::new(AnswerTable { answers: Vec::new(), width: answer_width }),
            executor_state: Mutex::new(TabledFunctionPatternExecutorState {
                pattern_executor,
                suspensions: QueryPatternSuspensions::new(),
                parameters,
            }),
        }
    }

    pub(crate) fn add_batch_to_table(&self, batch: FixedBatch) -> FixedBatch {
        if !batch.is_empty() {
            let mut deduplicated_batch = FixedBatch::new(batch.get_row(0).len() as u32);
            let mut table = self.table.write().unwrap();
            for row in batch {
                if table.try_add_row(row.as_reference()) {
                    deduplicated_batch.append(|mut write_to| write_to.copy_from_row(row))
                }
            }
            deduplicated_batch
        } else {
            batch
        }
    }
}

pub(crate) struct AnswerTable {
    // TODO: use a better data-structure. XSB has an "answer-trie" though a LinkedHashSet might do.
    answers: Vec<MaybeOwnedRow<'static>>,
    width: u32,
    // TODO: We need to be able to record the fact that a table is DONE
}

impl AnswerTable {
    pub(crate) fn len(&self) -> usize {
        self.answers.len()
    }

    pub(crate) fn read_batch_starting(&self, start_index: TableIndex) -> FixedBatch {
        let mut read_index = *start_index;
        let mut batch = FixedBatch::new(self.width);
        while !batch.is_full() && read_index < self.len() {
            batch.append(|mut write_to| {
                write_to
                    .copy_from_row(self.answers.get(read_index).map(|row| row.as_reference()).unwrap().as_reference());
            });
            read_index += 1;
        }
        batch
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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) struct CallKey {
    pub(crate) function_id: FunctionID,
    pub(crate) arguments: MaybeOwnedRow<'static>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct TableIndex(pub(crate) usize);
impl std::ops::Deref for TableIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TableIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
