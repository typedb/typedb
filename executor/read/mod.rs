/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::{function::ExecutableFunctionRegistry, match_::planner::match_executable::MatchExecutable};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    profile::QueryProfile,
    read::{
        pattern_executor::{BranchIndex, ExecutorIndex, PatternExecutor},
        tabled_call_executor::TabledCallExecutor,
        tabled_functions::TableIndex,
    },
    row::MaybeOwnedRow,
};

mod collecting_stage_executor;
pub(super) mod control_instruction;
pub mod expression_executor;
mod immediate_executor;
pub(crate) mod nested_pattern_executor;
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

#[derive(Debug)]
pub(crate) enum PatternSuspension {
    AtTabledCall(TabledCallSuspension),
    AtNestedPattern(NestedPatternSuspension),
}

impl PatternSuspension {
    fn depth(&self) -> usize {
        match self {
            PatternSuspension::AtTabledCall(tabled_call) => tabled_call.depth,
            PatternSuspension::AtNestedPattern(nested) => nested.depth,
        }
    }
}

#[derive(Debug)]
pub(super) struct TabledCallSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) depth: usize,
    pub(crate) input_row: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

#[derive(Debug)]
pub(super) struct NestedPatternSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) depth: usize,
    pub(crate) branch_index: BranchIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
}

#[derive(Debug)]
pub(super) struct QueryPatternSuspensions {
    current_depth: usize,
    suspending_patterns_tree: Vec<PatternSuspension>,
    restoring_patterns_tree: Vec<PatternSuspension>, //Peekable<std::vec::IntoIter<SuspendPoint>>,
}

#[derive(Debug, PartialEq, Eq)]
struct SuspensionCount(usize);

impl QueryPatternSuspensions {
    pub(crate) fn new() -> Self {
        Self { current_depth: 0, suspending_patterns_tree: Vec::new(), restoring_patterns_tree: Vec::new() }
    }

    pub(super) fn prepare_restoring_from_suspending(&mut self) {
        debug_assert!(self.restoring_patterns_tree.is_empty());
        self.restoring_patterns_tree.clear();
        std::mem::swap(&mut self.restoring_patterns_tree, &mut self.suspending_patterns_tree);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.suspending_patterns_tree.is_empty()
    }

    pub(crate) fn current_depth(&self) -> usize {
        self.current_depth
    }

    fn record_nested_pattern_entry(&mut self) -> SuspensionCount {
        self.current_depth += 1;
        SuspensionCount(self.suspending_patterns_tree.len())
    }

    fn record_nested_pattern_exit(&mut self) -> SuspensionCount {
        self.current_depth -= 1;
        SuspensionCount(self.suspending_patterns_tree.len())
    }

    fn push_nested(
        &mut self,
        executor_index: ExecutorIndex,
        branch_index: BranchIndex,
        input_row: MaybeOwnedRow<'static>,
    ) {
        self.suspending_patterns_tree.push(PatternSuspension::AtNestedPattern(NestedPatternSuspension {
            depth: self.current_depth,
            executor_index,
            branch_index,
            input_row,
        }))
    }

    fn push_tabled_call(&mut self, executor_index: ExecutorIndex, tabled_call_executor: &TabledCallExecutor) {
        self.suspending_patterns_tree
            .push(tabled_call_executor.create_suspension_at(executor_index, self.current_depth))
    }

    fn next_restore_point_at_current_depth(&mut self) -> Option<PatternSuspension> {
        let has_next = if let Some(point) = self.restoring_patterns_tree.last() {
            point.depth() == self.current_depth
        } else {
            false
        };
        if has_next {
            self.restoring_patterns_tree.pop()
        } else {
            None
        }
    }
}
