/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{function::ExecutableFunctionRegistry, match_::planner::match_executable::MatchExecutable},
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;
use utils::deref_for_trivial_struct;

use crate::{profile::QueryProfile, read::pattern_executor::PatternExecutor};

mod collecting_stage_executor;
pub(super) mod control_instruction;
pub mod expression_executor;
mod immediate_executor;
pub(crate) mod nested_pattern_executor;
pub(crate) mod pattern_executor;
pub(crate) mod step_executor;
mod stream_modifier;
pub(super) mod suspension;
pub(crate) mod tabled_call_executor;
pub mod tabled_functions;

// And use the below one instead

#[derive(Debug, Copy, Clone)]
pub(crate) struct BranchIndex(pub usize);
deref_for_trivial_struct!(BranchIndex => usize);
#[derive(Debug, Copy, Clone)]
pub(crate) struct ExecutorIndex(pub usize);

impl ExecutorIndex {
    fn next(&self) -> ExecutorIndex {
        ExecutorIndex(self.0 + 1)
    }
}
deref_for_trivial_struct!(ExecutorIndex => usize);

pub(super) fn create_pattern_executor_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
    profile: &QueryProfile,
) -> Result<PatternExecutor, Box<ConceptReadError>> {
    let executors = step_executor::create_executors_for_match(
        snapshot,
        thing_manager,
        function_registry,
        profile,
        match_executable,
    )?;
    Ok(PatternExecutor::new(match_executable.executable_id(), executors))
}
