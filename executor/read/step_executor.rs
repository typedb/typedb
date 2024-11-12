/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{
        function::{ExecutableFunction, ExecutableReturn, FunctionTablingType},
        match_::planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{ExecutionStep, MatchExecutable},
        },
        pipeline::ExecutableStage,
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::read::{
    collecting_stage_executor::CollectingStageExecutor, immediate_executor::ImmediateExecutor,
    nested_pattern_executor::NestedPatternExecutor, pattern_executor::PatternExecutor,
    stream_modifier::StreamModifierExecutor, tabled_call_executor::TabledCallExecutor,
};

pub(super) enum StepExecutors {
    Immediate(ImmediateExecutor),
    Nested(NestedPatternExecutor),
    StreamModifier(StreamModifierExecutor),
    CollectingStage(CollectingStageExecutor),
    TabledCall(TabledCallExecutor),
    ReshapeForReturn(Vec<VariablePosition>),
}

impl StepExecutors {
    pub(crate) fn unwrap_immediate(&mut self) -> &mut ImmediateExecutor {
        match self {
            StepExecutors::Immediate(step) => step,
            _ => panic!("bad unwrap"),
        }
    }

    pub(crate) fn unwrap_nested(&mut self) -> &mut NestedPatternExecutor {
        match self {
            StepExecutors::Nested(step) => step,
            _ => panic!("bad unwrap"),
        }
    }

    pub(crate) fn unwrap_tabled_call(&mut self) -> &mut TabledCallExecutor {
        match self {
            StepExecutors::TabledCall(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_stream_modifier(&mut self) -> &mut StreamModifierExecutor {
        match self {
            StepExecutors::StreamModifier(step) => step,
            _ => panic!("bad unwrap"),
        }
    }

    pub(crate) fn unwrap_collecting_stage(&mut self) -> &mut CollectingStageExecutor {
        match self {
            StepExecutors::CollectingStage(step) => step,
            _ => panic!("bad unwrap"),
        }
    }

    pub(crate) fn unwrap_reshape(&self) -> &Vec<VariablePosition> {
        match self {
            StepExecutors::ReshapeForReturn(return_positions) => return_positions,
            _ => panic!("bad unwrap"),
        }
    }
}

pub(crate) fn create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let mut steps = Vec::with_capacity(match_executable.steps().len());
    for step in match_executable.steps() {
        match step {
            ExecutionStep::Intersection(inner) => {
                let step = ImmediateExecutor::new_intersection(inner, snapshot, thing_manager)?;
                steps.push(step.into());
            }
            ExecutionStep::UnsortedJoin(inner) => {
                let step = ImmediateExecutor::new_unsorted_join(inner)?;
                steps.push(step.into());
            }
            ExecutionStep::Assignment(inner) => {
                let step = ImmediateExecutor::new_assignment(inner)?;
                steps.push(step.into());
            }
            ExecutionStep::Check(inner) => {
                let step = ImmediateExecutor::new_check(inner)?;
                steps.push(step.into());
            }
            ExecutionStep::Negation(negation_step) => {
                // I shouldn't need to pass recursive here since it's stratified
                let inner =
                    create_executors_for_match(snapshot, thing_manager, function_registry, &negation_step.negation)?;
                steps.push(NestedPatternExecutor::new_negation(PatternExecutor::new(inner)).into())
            }
            ExecutionStep::FunctionCall(function_call) => {
                let function = function_registry.get(function_call.function_id.clone());
                if function.is_tabled == FunctionTablingType::Tabled {
                    let executor = TabledCallExecutor::new(
                        function_call.function_id.clone(),
                        function_call.arguments.clone(),
                        function_call.assigned.clone(),
                        function_call.output_width,
                    );
                    steps.push(StepExecutors::TabledCall(executor))
                } else {
                    let inner_executors =
                        create_executors_for_function(snapshot, thing_manager, function_registry, function)?;
                    let inner = PatternExecutor::new(inner_executors);
                    let step = NestedPatternExecutor::new_inlined_function(
                        inner,
                        function_call,
                        function.parameter_registry.clone(),
                    );
                    steps.push(step.into())
                }
            }
            ExecutionStep::Disjunction(step) => {
                // I shouldn't need to pass recursive here since it's stratified
                let branches = step
                    .branches
                    .iter()
                    .map(|branch_executable| {
                        let executors =
                            create_executors_for_match(snapshot, thing_manager, function_registry, &branch_executable)?;
                        Ok::<_, Box<_>>(PatternExecutor::new(executors))
                    })
                    .try_collect()?;
                let inner_step = NestedPatternExecutor::new_disjunction(
                    branches,
                    step.selected_variables.clone(),
                    step.output_width,
                )
                .into();
                // Hack: wrap it in a distinct
                let step = StepExecutors::StreamModifier(StreamModifierExecutor::new_distinct(
                    PatternExecutor::new(vec![inner_step]),
                    step.output_width,
                ));
                steps.push(step);
            }
            ExecutionStep::Optional(_) => todo!(),
        };
    }
    Ok(steps)
}

pub(crate) fn create_executors_for_function(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_function: &ExecutableFunction,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let executable_stages = &executable_function.executable_stages;
    let mut steps = create_executors_for_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        executable_stages,
        executable_stages.len() - 1,
    )?;
    match &executable_function.returns {
        ExecutableReturn::Stream(positions) => {
            steps.push(StepExecutors::ReshapeForReturn(positions.clone()));
        }
        _ => todo!(),
    }
    Ok(steps)
}

pub(super) fn create_executors_for_pipeline_stages(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_stages: &Vec<ExecutableStage>,
    at_index: usize,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let mut previous_stage_steps = if at_index > 0 {
        create_executors_for_pipeline_stages(
            snapshot,
            thing_manager,
            function_registry,
            executable_stages,
            at_index - 1,
        )?
    } else {
        vec![]
    };

    match &executable_stages[at_index] {
        ExecutableStage::Match(match_executable) => {
            let mut match_stages =
                create_executors_for_match(snapshot, thing_manager, function_registry, match_executable)?;
            previous_stage_steps.append(&mut match_stages);
            Ok(previous_stage_steps)
        }
        ExecutableStage::Select(_) => todo!(),
        ExecutableStage::Offset(offset_executable) => {
            let step = StreamModifierExecutor::new_offset(
                PatternExecutor::new(previous_stage_steps),
                offset_executable.offset,
            );
            Ok(vec![step.into()])
        }
        ExecutableStage::Limit(limit_executable) => {
            let step =
                StreamModifierExecutor::new_limit(PatternExecutor::new(previous_stage_steps), limit_executable.limit);
            Ok(vec![step.into()])
        }
        ExecutableStage::Require(_) => todo!(),
        ExecutableStage::Sort(sort_executable) => {
            let step = CollectingStageExecutor::new_sort(PatternExecutor::new(previous_stage_steps), sort_executable);
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Reduce(reduce_executable) => {
            let step = CollectingStageExecutor::new_reduce(
                PatternExecutor::new(previous_stage_steps),
                reduce_executable.clone(),
            );
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Insert(_) | ExecutableStage::Delete(_) => {
            todo!(
                "Currently unreachable. Accept flag for whether this is a write pipeline & port the write stages here."
            )
        }
    }
}
