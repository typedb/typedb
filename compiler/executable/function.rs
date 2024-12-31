/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{function_signature::FunctionID, ParameterRegistry},
};
use typeql::schema::definable::function::SingleSelector;

use crate::{
    annotation::{
        function::{AnnotatedFunction, AnnotatedFunctionReturn},
        pipeline::AnnotatedStage,
    },
    executable::{
        match_::planner::function_plan::ExecutableFunctionRegistry,
        next_executable_id,
        pipeline::{compile_pipeline_stages, ExecutableStage},
        reduce::ReduceRowsExecutable,
        ExecutableCompilationError,
    },
    VariablePosition,
};

#[derive(Debug, Clone)]
pub struct ExecutableFunction {
    pub executable_id: u64,
    pub executable_stages: Vec<ExecutableStage>,
    pub argument_positions: HashMap<Variable, VariablePosition>,
    pub returns: ExecutableReturn,
    pub is_tabled: FunctionTablingType,
    pub parameter_registry: Arc<ParameterRegistry>,
    // pub plan_cost: f64, // TODO: Where do we fit this in?
}

#[derive(Debug, Clone)]
pub enum ExecutableReturn {
    Stream(Vec<VariablePosition>),
    Single(SingleSelector, Vec<VariablePosition>),
    Check,
    Reduce(Arc<ReduceRowsExecutable>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionTablingType {
    Tabled,
    Untabled,
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    schema_functions: &ExecutableFunctionRegistry, // Can't have preamble in them when you're compiling functions
    function: AnnotatedFunction,
    is_tabled: FunctionTablingType,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let AnnotatedFunction { variable_registry, parameter_registry, arguments, stages, return_, .. } = function;
    let (argument_positions, executable_stages) = compile_pipeline_stages(
        statistics,
        &variable_registry,
        schema_functions,
        stages,
        arguments.into_iter(),
        &return_.referenced_variables(),
    )?;

    let returns = compile_return_operation(&executable_stages, return_)?;
    Ok(ExecutableFunction {
        executable_id: next_executable_id(),
        executable_stages,
        argument_positions,
        returns,
        parameter_registry: Arc::new(parameter_registry),
        is_tabled,
    })
}

fn compile_return_operation(
    executable_stages: &[ExecutableStage],
    return_: AnnotatedFunctionReturn,
) -> Result<ExecutableReturn, ExecutableCompilationError> {
    let variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or_default();
    match return_ {
        AnnotatedFunctionReturn::Stream { variables, .. } => {
            Ok(ExecutableReturn::Stream(variables.iter().map(|var| variable_positions[var]).collect()))
        }
        AnnotatedFunctionReturn::Single { selector, variables, .. } => {
            Ok(ExecutableReturn::Single(selector, variables.iter().map(|var| variable_positions[var]).collect()))
        }
        | AnnotatedFunctionReturn::ReduceCheck {} => Ok(ExecutableReturn::Check),
        | AnnotatedFunctionReturn::ReduceReducer { instructions } => {
            let reductions = instructions.into_iter().map(|reducer| reducer.map(&variable_positions)).collect();
            Ok(ExecutableReturn::Reduce(Arc::new(ReduceRowsExecutable {
                reductions,
                input_group_positions: Vec::new(),
            })))
        }
    }
}

pub fn determine_tabling_requirements(
    functions: &HashMap<FunctionID, &AnnotatedFunction>,
    existing_requirements: &HashMap<FunctionID, FunctionTablingType>,
) -> HashMap<FunctionID, FunctionTablingType> {
    let mut cycle_detection: HashMap<FunctionID, TablingRequirement> =
        existing_requirements.iter().map(|(id, req)| {
            (id.clone(), match req {
                FunctionTablingType::Tabled => TablingRequirement::KnownTabled,
                FunctionTablingType::Untabled => TablingRequirement::KnownUntabled,
            })
        }).chain(
            functions.keys().map(|function_id| (function_id.clone(), TablingRequirement::Unexplored))
        ).collect();
    for function_id in functions.keys() {
        if cycle_detection[function_id] == TablingRequirement::Unexplored {
            determine_tabling_requirements_impl(functions, &mut cycle_detection, function_id)
        }
    }
    cycle_detection
        .into_iter()
        .map(|(function_id, tabling_requirement)| match tabling_requirement {
            TablingRequirement::KnownTabled => (function_id, FunctionTablingType::Tabled),
            TablingRequirement::KnownUntabled => (function_id, FunctionTablingType::Untabled),
            TablingRequirement::Unexplored | TablingRequirement::UnknownInStack => unreachable!(),
        })
        .collect()
}

#[derive(PartialEq, Eq)]
enum TablingRequirement {
    Unexplored,
    UnknownInStack,
    KnownTabled,
    KnownUntabled,
}

pub fn determine_tabling_requirements_impl(
    to_compile: &HashMap<FunctionID, &AnnotatedFunction>,
    cycle_detection: &mut HashMap<FunctionID, TablingRequirement>,
    function_id: &FunctionID,
) {
    // Recurse, and compile on our way out.
    match cycle_detection[function_id] {
        TablingRequirement::KnownTabled | TablingRequirement::KnownUntabled => {}
        TablingRequirement::UnknownInStack => {
            // We should only need to table these, and not other ones in the cycle.
            cycle_detection.insert(function_id.clone(), TablingRequirement::KnownTabled);
        }
        TablingRequirement::Unexplored => {
            cycle_detection.insert(function_id.clone(), TablingRequirement::UnknownInStack);
            let function = to_compile.get(function_id).unwrap();
            for called_id in all_calls_in_pipeline(&function.stages) {
                if to_compile.contains_key(&called_id) {
                    determine_tabling_requirements_impl(to_compile, cycle_detection, &called_id);
                }
            }
            match cycle_detection[function_id] {
                TablingRequirement::UnknownInStack => {
                    cycle_detection.insert(function_id.clone(), TablingRequirement::KnownUntabled);
                }
                TablingRequirement::KnownTabled => {}
                TablingRequirement::Unexplored | TablingRequirement::KnownUntabled => unreachable!(),
            }
        }
    }
}

fn all_calls_in_pipeline(stages: &[AnnotatedStage]) -> HashSet<FunctionID> {
    let match_stage_conjunctions = stages.iter().filter_map(|stage| match stage {
        AnnotatedStage::Match { block, .. } => Some(block.conjunction()),
        _ => None,
    });
    let mut call_accumulator = HashSet::new();
    for conjunction in match_stage_conjunctions {
        all_calls_in_conjunction(conjunction, &mut call_accumulator);
    }
    call_accumulator
}

fn all_calls_in_conjunction(conjunction: &Conjunction, call_accumulator: &mut HashSet<FunctionID>) {
    for constraint in conjunction.constraints() {
        if let Constraint::FunctionCallBinding(binding) = constraint {
            call_accumulator.insert(binding.function_call().function_id());
        }
    }
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for inner in disjunction.conjunctions() {
                    all_calls_in_conjunction(inner, call_accumulator);
                }
            }
            NestedPattern::Optional(optional) => {
                all_calls_in_conjunction(optional.conjunction(), call_accumulator);
            }
            NestedPattern::Negation(negation) => {
                all_calls_in_conjunction(negation.conjunction(), call_accumulator);
            }
        }
    }
}
