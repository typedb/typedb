/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::{ExecutionStep, MatchExecutable};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::read::{
    immediate_executor::ImmediateExecutor, nested_pattern_executor::NestedPatternExecutor,
};

pub(super) enum StepExecutors {
    Immediate(ImmediateExecutor),
    NestedPattern(NestedPatternExecutor),
}

impl StepExecutors {
    pub(crate) fn unwrap_executable(&mut self) -> &mut ImmediateExecutor {
        match self {
            StepExecutors::Immediate(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_nested_pattern_branch(&mut self) -> &mut NestedPatternExecutor {
        match self {
            StepExecutors::NestedPattern(step) => step,
            _ => unreachable!(),
        }
    }
}

pub(super) fn create_executors_recursive(
    match_executable: &MatchExecutable,
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    let mut steps = Vec::with_capacity(match_executable.steps().len());
    for step in match_executable.steps() {
        let step =
            match step {
                ExecutionStep::Intersection(_) => {
                    StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::UnsortedJoin(_) => {
                    StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Assignment(_) => {
                    StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Check(_) => {
                    StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Negation(negation_step) => StepExecutors::NestedPattern(
                    // TODO: I'd like to refactor the immediate branches to this pattern too.
                    NestedPatternExecutor::new_negation(negation_step, snapshot, thing_manager)?,
                ),
                _ => todo!(),
            };
        steps.push(step);
    }
    Ok(steps)
}
