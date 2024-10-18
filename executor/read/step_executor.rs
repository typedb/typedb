/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{
        function::{ExecutableFunction, ExecutableReturn},
        match_::planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{ExecutionStep, MatchExecutable},
        },
        pipeline::ExecutableStage,
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::read::{
    immediate_executor::ImmediateExecutor,
    nested_pattern_executor::{InlinedFunctionMapper, NegationMapper, NestedPatternBranch, NestedPatternExecutor},
    pattern_executor::PatternExecutor,
};

pub(super) enum StepExecutors {
    Immediate(ImmediateExecutor),
    Branch(NestedPatternExecutor),
    ReshapeForReturn(Vec<VariablePosition>),
}

impl StepExecutors {
    pub(crate) fn unwrap_immediate(&mut self) -> &mut ImmediateExecutor {
        match self {
            StepExecutors::Immediate(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_nested_pattern_branch(&mut self) -> &mut NestedPatternExecutor {
        match self {
            StepExecutors::Branch(step) => step,
            _ => unreachable!(),
        }
    }
}

pub(super) fn create_executors_recursive(
    match_executable: &MatchExecutable,
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    let mut steps = Vec::with_capacity(match_executable.steps().len());
    for step in match_executable.steps() {
        let step = match step {
            ExecutionStep::Intersection(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::UnsortedJoin(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Assignment(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Check(_) => StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::Negation(negation_step) => {
                let inner =
                    PatternExecutor::build(&negation_step.negation, snapshot, thing_manager, function_registry)?;
                StepExecutors::Branch(NestedPatternExecutor::Negation(
                    [NestedPatternBranch::new(inner)],
                    NegationMapper,
                ))
            }
            ExecutionStep::FunctionCall(function_call) => {
                let function = function_registry.get(function_call.function_id.clone());
                if function.is_tabled {
                    todo!()
                } else {
                    debug_assert!(!function.is_tabled);
                    let ExecutableStage::Match(match_) = &function.executable_stages[0] else {
                        todo!("Pipelines in functions")
                    };
                    let inner = PatternExecutor::build(match_, snapshot, thing_manager, function_registry)?;
                    let mapper = InlinedFunctionMapper::new(
                        function_call.arguments.clone(),
                        function_call.assigned.clone(),
                        function_call.output_width,
                    );
                    StepExecutors::Branch(NestedPatternExecutor::InlinedFunction(
                        [NestedPatternBranch::new(inner)],
                        mapper,
                    ))
                }
            }
            _ => todo!(),
        };
        steps.push(step);
    }
    Ok(steps)
}

pub(super) fn create_executors_for_function(
    executable_function: &ExecutableFunction,
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    // TODO: Support full pipelines
    debug_assert!(executable_function.executable_stages.len() == 1);
    let ExecutableStage::Match(match_executable) = &executable_function.executable_stages[0] else {
        unreachable!();
    };
    let mut steps = create_executors_recursive(match_executable, snapshot, thing_manager, function_registry)?;

    // TODO: Add table writing step.
    match &executable_function.returns {
        ExecutableReturn::Stream(positions) => {
            steps.push(StepExecutors::ReshapeForReturn(positions.clone()));
        }
        _ => todo!(),
    }
    Ok(steps)
}
