/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use compiler::executable::{
    match_::planner::{function_plan::ExecutableFunctionRegistry, match_executable::MatchExecutable},
    pipeline::ExecutableStage,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pipeline::function_signature::FunctionID;
use storage::snapshot::ReadableSnapshot;

use crate::{
    read::{
        pattern_executor::{BranchIndex, ExecutorIndex, PatternExecutor},
        step_executor::create_executors_for_pipeline_stages,
        tabled_functions::TableIndex,
    },
    row::MaybeOwnedRow,
};

mod collecting_stage_executor;
pub(super) mod control_instruction;
pub mod expression_executor;
mod immediate_executor;
mod nested_pattern_executor;
pub(crate) mod pattern_executor;
pub(crate) mod step_executor;
mod stream_modifier;
pub(crate) mod tabled_call_executor;
pub mod tabled_functions;

// And use the below one instead
pub(super) fn TODO_REMOVE_create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
) -> Result<PatternExecutor, ConceptReadError> {
    let executors = step_executor::create_executors_for_match(
        snapshot,
        thing_manager,
        function_registry,
        match_executable,
        &mut HashSet::new(),
    )?;
    Ok(PatternExecutor::new(executors))
}

pub(super) fn create_executors_for_pipeline(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_stages: &Vec<ExecutableStage>,
    tmp__recursion_validation: &mut HashSet<FunctionID>,
) -> Result<PatternExecutor, ConceptReadError> {
    let executors = create_executors_for_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        executable_stages,
        executable_stages.len() - 1,
        tmp__recursion_validation,
    )?;
    Ok(PatternExecutor::new(executors))
}

pub(crate) enum SuspendPoint {
    TabledCall(TabledCallSuspension),
    Nested(NestedSuspension),
}

impl SuspendPoint {
    fn new_tabled_call(
        executor_index: ExecutorIndex,
        next_table_row: TableIndex,
        input_row: MaybeOwnedRow<'static>,
    ) -> Self {
        Self::TabledCall(TabledCallSuspension { executor_index, next_table_row, input_row })
    }

    fn new_nested(executor_index: ExecutorIndex, branch_index: BranchIndex, input_row: MaybeOwnedRow<'static>) -> Self {
        Self::Nested(NestedSuspension { executor_index, branch_index, input_row })
    }
}

#[derive(Debug)]
pub(super) struct TabledCallSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

#[derive(Debug)]
pub(super) struct NestedSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) branch_index: BranchIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
}
