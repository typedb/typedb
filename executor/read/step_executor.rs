/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

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
use ir::pipeline::function_signature::FunctionID;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::read::{
    collecting_stage_executor::CollectingStageExecutor,
    immediate_executor::ImmediateExecutor,
    nested_pattern_executor::{
        IdentityMapper, InlinedFunctionMapper, LimitMapper, NegationMapper, NestedPatternBranch, NestedPatternExecutor,
        OffsetMapper,
    },
    pattern_executor::PatternExecutor,
};

pub(super) enum StepExecutors {
    Immediate(ImmediateExecutor),
    Branch(NestedPatternExecutor),
    CollectingStage(CollectingStageExecutor),
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

    pub(crate) fn unwrap_collecting_stage(&mut self) -> &mut CollectingStageExecutor {
        match self {
            StepExecutors::CollectingStage(step) => step,
            _ => unreachable!(),
        }
    }
}

pub(super) fn create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
    tmp__recursion_validation: &mut HashSet<FunctionID>,
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
                println!("Negation_Plan:\n{:?}", &negation_step.negation);
                // I shouldn't need to pass recursive here since it's stratified
                let inner = create_executors_for_match(
                    snapshot,
                    thing_manager,
                    function_registry,
                    &negation_step.negation,
                    tmp__recursion_validation,
                )?;
                StepExecutors::Branch(NestedPatternExecutor::Negation(
                    [NestedPatternBranch::new(PatternExecutor::new(inner))],
                    NegationMapper,
                ))
            }
            ExecutionStep::FunctionCall(function_call) => {
                let function = function_registry.get(function_call.function_id.clone());
                if function.is_tabled {
                    todo!()
                } else {
                    if tmp__recursion_validation.contains(&function_call.function_id) {
                        todo!(
                            "Recursive functions are unsupported in this release. Continuing would overflow the stack"
                        )
                    } else {
                        tmp__recursion_validation.insert(function_call.function_id.clone());
                    }
                    let inner_executors = create_executors_for_pipeline_stages(
                        snapshot,
                        thing_manager,
                        function_registry,
                        &function.executable_stages,
                        &function.executable_stages.len() - 1,
                        tmp__recursion_validation,
                    )?;
                    let inner = PatternExecutor::new(inner_executors);
                    tmp__recursion_validation.remove(&function_call.function_id);
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
            ExecutionStep::Disjunction(step) => {
                // I shouldn't need to pass recursive here since it's stratified
                let inner = step
                    .branches
                    .iter()
                    .map(|branch_executable| {
                        let executors = create_executors_for_match(
                            snapshot,
                            thing_manager,
                            function_registry,
                            &branch_executable,
                            tmp__recursion_validation,
                        )?;
                        Ok(NestedPatternBranch::new(PatternExecutor::new(executors)))
                    })
                    .try_collect()?;
                let inner_step = StepExecutors::Branch(NestedPatternExecutor::Disjunction(inner, IdentityMapper));
                // Hack: wrap it in a distinct
                StepExecutors::CollectingStage(CollectingStageExecutor::new_distinct(PatternExecutor::new(vec![
                    inner_step,
                ])))
            }
            ExecutionStep::Optional(_) => todo!(),
        };
        steps.push(step);
    }
    Ok(steps)
}

pub(super) fn create_executors_for_function(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_function: &ExecutableFunction,
    tmp__recursion_validation: &mut HashSet<FunctionID>,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    // TODO: Support full pipelines
    debug_assert!(executable_function.executable_stages.len() == 1);
    let executable_stages = &executable_function.executable_stages;
    let mut steps = create_executors_for_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        executable_stages,
        executable_stages.len() - 1,
        tmp__recursion_validation,
    )?;

    // TODO: Add table writing step.
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
    tmp__recursion_validation: &mut HashSet<FunctionID>,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    let mut previous_stage_steps = if at_index > 0 {
        create_executors_for_pipeline_stages(
            snapshot,
            thing_manager,
            function_registry,
            executable_stages,
            at_index - 1,
            tmp__recursion_validation,
        )?
    } else {
        vec![]
    };

    match &executable_stages[at_index] {
        ExecutableStage::Match(match_executable) => {
            let mut match_stages = create_executors_for_match(
                snapshot,
                thing_manager,
                function_registry,
                match_executable,
                tmp__recursion_validation,
            )?;
            previous_stage_steps.append(&mut match_stages);
            Ok(previous_stage_steps)
        }
        ExecutableStage::Select(_) => todo!(),
        ExecutableStage::Offset(offset_executable) => {
            let inner = NestedPatternBranch::new(PatternExecutor::new(previous_stage_steps));
            let mapper = OffsetMapper::new(offset_executable.offset);
            let step = NestedPatternExecutor::Offset([inner], mapper);
            Ok(vec![StepExecutors::Branch(step)])
        }
        ExecutableStage::Limit(limit_executable) => {
            let inner = NestedPatternBranch::new(PatternExecutor::new(previous_stage_steps));
            let mapper = LimitMapper::new(limit_executable.limit);
            let step = NestedPatternExecutor::Limit([inner], mapper);
            Ok(vec![StepExecutors::Branch(step)])
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
        ExecutableStage::Insert(_) | ExecutableStage::Delete(_) => todo!("Or unreachable?"),
    }
}
