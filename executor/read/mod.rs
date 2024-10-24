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

use crate::read::{pattern_executor::PatternExecutor, step_executor::create_executors_for_pipeline_stages};
use crate::read::pattern_executor::{BranchIndex, InstructionIndex};
use crate::read::tabled_functions::TableIndex;
use crate::row::MaybeOwnedRow;

mod collecting_stage_executor;
pub mod expression_executor;
mod immediate_executor;
mod nested_pattern_executor;
pub(crate) mod pattern_executor;
mod step_executor;
pub(crate) mod tabled_functions;

// And use the below one instead
pub(super) fn TODO_REMOVE_create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
) -> Result<PatternExecutor, ConceptReadError> {
    eprintln!("--- Start creating executors for entry ---");
    let executors = step_executor::create_executors_for_match(
        snapshot,
        thing_manager,
        function_registry,
        match_executable,
        &mut HashSet::new(),
    )?;
    eprintln!("--- End creating executors for entry ---");
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
    fn new_tabled_call(instruction_index: InstructionIndex, next_table_row: TableIndex, input_row: MaybeOwnedRow<'static>) -> Self {
        Self::TabledCall(TabledCallSuspension { instruction_index, next_table_row, input_row } )
    }

    fn new_nested(instruction_index: InstructionIndex, branch_index: BranchIndex, input_row: MaybeOwnedRow<'static>) -> Self {
        Self::Nested(NestedSuspension { instruction_index, branch_index, input_row } )
    }
}

#[derive(Debug)]
pub(super) struct TabledCallSuspension {
    pub(crate) instruction_index: InstructionIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

#[derive(Debug)]
pub(super) struct NestedSuspension {
    pub(crate) instruction_index: InstructionIndex,
    pub(crate) branch_index: BranchIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
}
