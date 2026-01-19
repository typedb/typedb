/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::{
    function::ExecutableFunctionRegistry, match_::planner::conjunction_executable::ConjunctionExecutable,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use resource::profile::QueryProfile;
use storage::snapshot::ReadableSnapshot;

use crate::read::pattern_executor::PatternExecutor;

pub(crate) mod builtin_call_executor;
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

#[derive(Debug, Copy, Clone)]
pub(crate) struct BranchIndex(pub usize);
impl std::ops::Deref for BranchIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct ExecutorIndex(pub usize);

impl ExecutorIndex {
    fn next(&self) -> ExecutorIndex {
        ExecutorIndex(self.0 + 1)
    }
}

impl std::ops::Deref for ExecutorIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(super) fn create_pattern_executor_for_conjunction(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    conjunction_executable: &ConjunctionExecutable,
    profile: &QueryProfile,
) -> Result<PatternExecutor, Box<ConceptReadError>> {
    let executors = step_executor::create_executors_for_conjunction(
        snapshot,
        thing_manager,
        function_registry,
        profile,
        conjunction_executable,
    )?;
    Ok(PatternExecutor::new(conjunction_executable.executable_id(), executors))
}
