/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::executable::function::StronglyConnectedComponentID;
use crate::read::pattern_executor::{BranchIndex, ExecutorIndex};
use crate::read::tabled_call_executor::TabledCallExecutor;
use crate::read::tabled_functions::TableIndex;
use crate::row::MaybeOwnedRow;

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

#[derive(Debug, PartialEq, Eq)]
pub(super) struct SuspensionCount(usize);

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
pub(crate) struct QueryPatternSuspensions {
    scc: Option<StronglyConnectedComponentID>,
    current_depth: usize,
    suspending_patterns_tree: Vec<PatternSuspension>,
    restoring_patterns_tree: Vec<PatternSuspension>,
}

impl QueryPatternSuspensions {
    pub(crate) fn new_root() -> Self {
        Self { scc: None, current_depth: 0, suspending_patterns_tree: Vec::new(), restoring_patterns_tree: Vec::new() }
    }

    pub(crate) fn new_tabled_call(scc: StronglyConnectedComponentID) -> Self {
        Self {
            scc: Some(scc),
            current_depth: 0,
            suspending_patterns_tree: Vec::new(),
            restoring_patterns_tree: Vec::new(),
        }
    }

    pub(crate) fn scc(&self) -> Option<&StronglyConnectedComponentID> {
        return self.scc.as_ref();
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

    pub(super) fn record_nested_pattern_entry(&mut self) -> SuspensionCount {
        self.current_depth += 1;
        SuspensionCount(self.suspending_patterns_tree.len())
    }

    pub(super) fn record_nested_pattern_exit(&mut self) -> SuspensionCount {
        self.current_depth -= 1;
        SuspensionCount(self.suspending_patterns_tree.len())
    }

    pub(super) fn push_nested(
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

    pub(super) fn push_tabled_call(&mut self, executor_index: ExecutorIndex, tabled_call_executor: &TabledCallExecutor) {
        self.suspending_patterns_tree
            .push(tabled_call_executor.create_suspension_at(executor_index, self.current_depth))
    }

    pub(super) fn next_restore_point_at_current_depth(&mut self) -> Option<PatternSuspension> {
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
