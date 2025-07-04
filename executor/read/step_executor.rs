/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{
        function::{
            executable::{ExecutableFunction, ExecutableReturn},
            ExecutableFunctionRegistry, FunctionTablingType,
        },
        match_::planner::conjunction_executable::{ExecutionStep, ConjunctionExecutable},
        next_executable_id,
        pipeline::ExecutableStage,
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use error::{unimplemented_feature, UnimplementedFeature};
use itertools::Itertools;
use resource::profile::QueryProfile;
use storage::snapshot::ReadableSnapshot;
use typeql::schema::definable::function::SingleSelector;

use crate::{
    batch::FixedBatch,
    read::{
        collecting_stage_executor::CollectingStageExecutor,
        immediate_executor::ImmediateExecutor,
        nested_pattern_executor::{DisjunctionExecutor, InlinedCallExecutor, NegationExecutor},
        pattern_executor::PatternExecutor,
        stream_modifier::StreamModifierExecutor,
        tabled_call_executor::TabledCallExecutor,
    },
};

#[derive(Debug)]
pub enum StepExecutors {
    Immediate(ImmediateExecutor),
    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    InlinedCall(InlinedCallExecutor),
    TabledCall(TabledCallExecutor),
    StreamModifier(StreamModifierExecutor),
    CollectingStage(CollectingStageExecutor),
    ReshapeForReturn(ReshapeForReturnExecutor),
}

impl StepExecutors {
    pub(crate) fn unwrap_immediate(&mut self) -> &mut ImmediateExecutor {
        match self {
            StepExecutors::Immediate(step) => step,
            _ => panic!("bad unwrap. Expected Immediate"),
        }
    }

    pub(crate) fn unwrap_negation(&mut self) -> &mut NegationExecutor {
        match self {
            StepExecutors::Negation(step) => step,
            _ => panic!("bad unwrap. Expected Negation"),
        }
    }

    pub(crate) fn unwrap_disjunction(&mut self) -> &mut DisjunctionExecutor {
        match self {
            StepExecutors::Disjunction(step) => step,
            _ => panic!("bad unwrap. Expected Disjunction"),
        }
    }

    pub(crate) fn unwrap_inlined_call(&mut self) -> &mut InlinedCallExecutor {
        match self {
            StepExecutors::InlinedCall(step) => step,
            _ => panic!("bad unwrap. Expected InlinedCall"),
        }
    }

    pub(crate) fn unwrap_tabled_call(&mut self) -> &mut TabledCallExecutor {
        match self {
            StepExecutors::TabledCall(step) => step,
            _ => panic!("bad unwrap. Expected TabledCall"),
        }
    }

    pub(crate) fn unwrap_stream_modifier(&mut self) -> &mut StreamModifierExecutor {
        match self {
            StepExecutors::StreamModifier(step) => step,
            _ => panic!("bad unwrap. Expected StreamModifier"),
        }
    }

    pub(crate) fn unwrap_collecting_stage(&mut self) -> &mut CollectingStageExecutor {
        match self {
            StepExecutors::CollectingStage(step) => step,
            _ => panic!("bad unwrap. Expected CollectingStage"),
        }
    }

    pub(crate) fn unwrap_reshape(&self) -> &ReshapeForReturnExecutor {
        match self {
            StepExecutors::ReshapeForReturn(step) => step,
            _ => panic!("bad unwrap. Expected ReshapeForReturn"),
        }
    }
}

#[derive(Debug)]
pub struct ReshapeForReturnExecutor(Vec<VariablePosition>);

impl ReshapeForReturnExecutor {
    pub(super) fn map_output(&self, batch: FixedBatch) -> FixedBatch {
        let mut output_batch = FixedBatch::new(self.0.len() as u32);
        batch.into_iter().for_each(|row| output_batch.append(|mut out| out.copy_mapped(row, self.as_mapping())));
        output_batch
    }

    fn as_mapping(&self) -> impl Iterator<Item = (VariablePosition, VariablePosition)> + '_ {
        self.0.iter().enumerate().map(|(dst, src)| (*src, VariablePosition::new(dst as u32)))
    }
}

pub(crate) fn create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    query_profile: &QueryProfile,
    match_executable: &ConjunctionExecutable,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let stage_profile = query_profile.profile_stage(
        || format!("Match\n  ~ {}", match_executable.planner_statistics()),
        match_executable.executable_id(),
    );
    let mut steps = Vec::with_capacity(match_executable.steps().len());
    for (index, step) in match_executable.steps().iter().enumerate() {
        match step {
            ExecutionStep::Intersection(inner) => {
                let step_profile = stage_profile.extend_or_get(index, || {
                    format!("{}", inner.make_var_mapped(match_executable.variable_reverse_map()))
                });
                let step = ImmediateExecutor::new_intersection(inner, snapshot, thing_manager, step_profile)?;
                steps.push(step.into());
            }
            ExecutionStep::UnsortedJoin(inner) => {
                let step_profile = stage_profile.extend_or_get(index, || format!("{}", inner));
                let step = ImmediateExecutor::new_unsorted_join(inner, step_profile)?;
                steps.push(step.into());
            }
            ExecutionStep::Assignment(inner) => {
                let step_profile = stage_profile.extend_or_get(index, || format!("{}", inner));
                let step = ImmediateExecutor::new_assignment(inner, step_profile)?;
                steps.push(step.into());
            }
            ExecutionStep::Check(inner) => {
                let step_profile = stage_profile.extend_or_get(index, || {
                    format!("{}", inner.make_var_mapped(match_executable.variable_reverse_map()))
                });
                let step = ImmediateExecutor::new_check(inner, step_profile)?;
                steps.push(step.into());
            }
            ExecutionStep::Negation(negation_step) => {
                // NOTE: still create the profile so each step has an entry in the profile, even if unused
                let _step_profile = stage_profile.extend_or_get(index, || format!("{}", negation_step));
                let inner = create_executors_for_match(
                    snapshot,
                    thing_manager,
                    function_registry,
                    query_profile,
                    &negation_step.negation,
                )?;
                // I shouldn't need to pass recursive here since it's stratified
                steps.push(
                    NegationExecutor::new(PatternExecutor::new(negation_step.negation.executable_id(), inner)).into(),
                )
            }
            ExecutionStep::FunctionCall(function_call) => {
                // NOTE: still create the profile so each step has an entry in the profile, even if unused
                let _step_profile = stage_profile.extend_or_get(index, || format!("{}", function_call));

                let function = function_registry.get(&function_call.function_id).unwrap();
                if let FunctionTablingType::Tabled(_) = function.tabling_type {
                    let executor = TabledCallExecutor::new(
                        function_call.function_id.clone(),
                        function_call.arguments.clone(),
                        function_call.assigned.clone(),
                        function_call.output_width,
                    );
                    steps.push(StepExecutors::TabledCall(executor))
                } else {
                    let inner_executors = create_executors_for_function(
                        snapshot,
                        thing_manager,
                        function_registry,
                        query_profile,
                        function,
                    )?;
                    let inner = PatternExecutor::new(function.executable_id, inner_executors);
                    let step = InlinedCallExecutor::new(inner, function_call, function.parameter_registry.clone());
                    steps.push(step.into())
                }
            }
            ExecutionStep::Disjunction(step) => {
                // NOTE: still create the profile so each step has an entry in the profile, even if unused
                let _step_profile = stage_profile.extend_or_get(index, || format!("{}", step));

                // I shouldn't need to pass recursive here since it's stratified
                let branches: Vec<PatternExecutor> = step
                    .branches
                    .iter()
                    .map(|branch_executable| {
                        let executors = create_executors_for_match(
                            snapshot,
                            thing_manager,
                            function_registry,
                            query_profile,
                            branch_executable,
                        )?;
                        Ok::<_, Box<_>>(PatternExecutor::new(branch_executable.executable_id(), executors))
                    })
                    .try_collect()?;
                let inner_step = DisjunctionExecutor::new(
                    step.branch_ids.clone(),
                    branches,
                    step.selected_variables.clone(),
                    step.output_width,
                )
                .into();
                // Hack: wrap it in a distinct
                let step = StepExecutors::StreamModifier(StreamModifierExecutor::new_distinct(
                    PatternExecutor::new(next_executable_id(), vec![inner_step]),
                    step.output_width,
                ));
                steps.push(step);
            }
            ExecutionStep::Optional(_) => unimplemented_feature!(Optionals),
        };
    }
    Ok(steps)
}

pub(crate) fn create_executors_for_function(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    query_profile: &QueryProfile,
    executable_function: &ExecutableFunction,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let executable_stages = &executable_function.executable_stages;
    let mut steps = create_executors_for_function_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        query_profile,
        executable_stages,
        executable_stages.len() - 1,
    )?;
    match &executable_function.returns {
        ExecutableReturn::Stream(positions) => {
            steps.push(StepExecutors::ReshapeForReturn(ReshapeForReturnExecutor(positions.clone())));
            Ok(steps)
        }
        ExecutableReturn::Single(selector, positions) => {
            steps.push(StepExecutors::ReshapeForReturn(ReshapeForReturnExecutor(positions.clone())));
            let pattern_executor = PatternExecutor::new(executable_function.executable_id, steps);
            let step = match selector {
                SingleSelector::First => StreamModifierExecutor::new_first(pattern_executor),
                SingleSelector::Last => StreamModifierExecutor::new_last(pattern_executor),
            };
            Ok(vec![step.into()])
        }
        ExecutableReturn::Check => {
            let pattern_executor = PatternExecutor::new(executable_function.executable_id, steps);
            let step = StreamModifierExecutor::new_check(pattern_executor);
            Ok(vec![step.into()])
        }
        ExecutableReturn::Reduce(executable) => {
            let step = CollectingStageExecutor::new_reduce(
                PatternExecutor::new(executable_function.executable_id, steps),
                executable.clone(),
            );
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
    }
}

pub(super) fn create_executors_for_function_pipeline_stages(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    query_profile: &QueryProfile,
    executable_stages: &[ExecutableStage],
    at_index: usize,
) -> Result<Vec<StepExecutors>, Box<ConceptReadError>> {
    let mut previous_stage_steps = if at_index > 0 {
        create_executors_for_function_pipeline_stages(
            snapshot,
            thing_manager,
            function_registry,
            query_profile,
            executable_stages,
            at_index - 1,
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
                query_profile,
                match_executable,
            )?;
            previous_stage_steps.append(&mut match_stages);
            Ok(previous_stage_steps)
        }
        ExecutableStage::Select(select_executable) => {
            let removed_positions: Vec<VariablePosition> =
                select_executable.removed_positions.iter().cloned().collect();
            let step = StreamModifierExecutor::new_select(
                // TODO: not sure if these are correct new executable IDs or should be different?
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                removed_positions,
            );
            Ok(vec![step.into()])
        }
        ExecutableStage::Offset(offset_executable) => {
            let step = StreamModifierExecutor::new_offset(
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                offset_executable.offset,
            );
            Ok(vec![step.into()])
        }
        ExecutableStage::Limit(limit_executable) => {
            let step = StreamModifierExecutor::new_limit(
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                limit_executable.limit,
            );
            Ok(vec![step.into()])
        }
        ExecutableStage::Require(_) => Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: UnimplementedFeature::PipelineStageInFunction("require"),
        })),
        ExecutableStage::Distinct(distinct_executable) => {
            // Complete this sentence for
            let step = StreamModifierExecutor::new_distinct(
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                distinct_executable.output_row_mapping.values().len() as u32,
            );
            Ok(vec![step.into()])
        }
        ExecutableStage::Sort(sort_executable) => {
            let step = CollectingStageExecutor::new_sort(
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                sort_executable,
            );
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Reduce(reduce_stage_executable) => {
            let step = CollectingStageExecutor::new_reduce(
                PatternExecutor::new(next_executable_id(), previous_stage_steps),
                reduce_stage_executable.reduce_rows_executable.clone(),
            );
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Insert(_)
        | ExecutableStage::Update(_)
        | ExecutableStage::Put(_)
        | ExecutableStage::Delete(_) => {
            unreachable!(
                "Not allowed in function pipelines. TODO: Accept flag for whether this is a write pipeline & port the write stages here."
            )
        }
    }
}
