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
    pattern_executor::BranchIndex, step_executors::StepExecutor, subpattern_executor::SubPatternExecutor,
};

pub(super) enum PatternInstruction {
    Executable(StepExecutor),
    SubPattern(SubPatternExecutor),
}

impl PatternInstruction {
    pub(crate) fn unwrap_executable(&mut self) -> &mut StepExecutor {
        match self {
            PatternInstruction::Executable(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_subpattern_branch(&mut self) -> &mut SubPatternExecutor {
        match self {
            PatternInstruction::SubPattern(step) => step,
            _ => unreachable!(),
        }
    }
}

pub(super) fn create_executors_recursive(
    match_executable: &MatchExecutable,
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
) -> Result<Vec<PatternInstruction>, ConceptReadError> {
    let mut steps = Vec::new();
    for step in match_executable.steps() {
        let step =
            match step {
                ExecutionStep::Intersection(_) => {
                    PatternInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::UnsortedJoin(_) => {
                    PatternInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Assignment(_) => {
                    PatternInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Check(_) => {
                    PatternInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
                }
                ExecutionStep::Negation(negation_step) => PatternInstruction::SubPattern(
                    SubPatternExecutor::new_negation(negation_step, snapshot, thing_manager)?,
                ),
                _ => todo!(),
            };
        steps.push(step);
    }
    // TODO: Add a table step?
    Ok(steps)
}
