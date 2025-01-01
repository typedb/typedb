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
    pipeline::{
        function_signature::{FunctionID, FunctionIDAPI},
        ParameterRegistry,
    },
};
use itertools::Itertools;
use typeql::schema::definable::function::SingleSelector;

use crate::{
    annotation::{
        function::{AnnotatedFunction, AnnotatedFunctionReturn},
        pipeline::AnnotatedStage,
    },
    executable::{
        match_::planner::{function_plan::ExecutableFunctionRegistry, vertex::Cost},
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
    pub single_call_cost: Cost, // TODO: Where do we fit this in?
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

pub trait FunctionCallCostProvider {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost;
}

struct FunctionPlanner<'a, FIDType: FunctionIDAPI> {
    cached_plans: &'a ExecutableFunctionRegistry,
    to_compile: HashMap<FIDType, AnnotatedFunction>,
    completed: HashMap<FIDType, ExecutableFunction>,
    must_table: HashSet<FIDType>,
}

impl<'a, FIDType: FunctionIDAPI> FunctionPlanner<'a, FIDType> {
    fn new(cached_plans: &'a ExecutableFunctionRegistry, to_compile: HashMap<FIDType, AnnotatedFunction>) -> Self {
        FunctionPlanner { cached_plans, to_compile, completed: HashMap::new(), must_table: HashSet::new() }
    }

    pub(crate) fn get_executable_function(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        if let Some(plan) = self.cached_plans.get(function_id) {
            Some(plan)
        } else if let Ok(key) = &FIDType::try_from(function_id.clone()) {
            self.completed.get(&key)
        } else {
            None
        }
    }

    fn cycle_breaking_cost(&self) -> Cost {
        Cost { cost: 1.0, io_ratio: 1.0 } // TODO
    }
}

impl<'a, FIDType: FunctionIDAPI> FunctionCallCostProvider for FunctionPlanner<'a, FIDType> {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        if let Some(function) = self.get_executable_function(function_id) {
            function.single_call_cost.clone()
        } else {
            debug_assert!(matches!(
                FIDType::try_from(function_id.clone()).map(|id| self.must_table.contains(&id)),
                Ok(true)
            ));
            self.cycle_breaking_cost()
        }
    }
}

pub(crate) fn compile_functions<FIDType: FunctionIDAPI>(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    mut to_compile: HashMap<FIDType, AnnotatedFunction>,
) -> Result<HashMap<FIDType, ExecutableFunction>, ExecutableCompilationError> {
    // TODO: Cache compiled schema functions?
    #[cfg(debug_assertions)]
    let debug__to_compile_count = to_compile.len();
    let mut planner = FunctionPlanner::new(cached_plans, to_compile);
    let mut cycle_detection = HashSet::new();
    while !planner.to_compile.is_empty() {
        let id = planner.to_compile.keys().find_or_first(|_| true).unwrap().clone();
        compile_functions_impl(statistics, &mut planner, &mut cycle_detection, id)?;
    }
    #[cfg(debug_assertions)]
    debug_assert!(planner.completed.len() == debug__to_compile_count);

    Ok(planner.completed)
}

fn compile_functions_impl<'a, FIDType: FunctionIDAPI>(
    statistics: &Statistics,
    planner: &mut FunctionPlanner<'a, FIDType>,
    cycle_detection: &mut HashSet<FIDType>,
    current: FIDType,
) -> Result<(), ExecutableCompilationError> {
    let function = planner.to_compile.remove(&current).unwrap();
    cycle_detection.insert(current.clone());
    let all_calls = all_calls_in_pipeline(function.stages.as_slice());
    // Plan all dependencies or cycle break.
    for called_fid in all_calls {
        if planner.get_executable_function(&called_fid).is_some() {
            continue;
        } else {
            let Ok(as_id) = FIDType::try_from(called_fid) else { unreachable!("Has to be in get_executable_function") };
            if cycle_detection.contains(&as_id) {
                planner.must_table.insert(as_id);
                // We compile this when we return all the way. The FunctionCostProvider should return a default cost for any uncompiled in must_table
            } else {
                debug_assert!(planner.to_compile.contains_key(&as_id));
                compile_functions_impl(statistics, planner, cycle_detection, as_id.clone())?;
            }
        }
    }
    cycle_detection.remove(&current); // I don't think we have to remove from cycle detection for correctness.

    // Now plan & compile this function.
    let must_table =
        if planner.must_table.contains(&current) { FunctionTablingType::Tabled } else { FunctionTablingType::Untabled };
    let compiled_function = compile_function(statistics, function, planner, must_table)?;
    planner.completed.insert(current.clone(), compiled_function);
    Ok(())
}

pub(crate) fn compile_single_untabled_function(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    to_compile: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let planner = FunctionPlanner::new(cached_plans, HashMap::<usize, _>::new());
    compile_function(statistics, to_compile, &planner, FunctionTablingType::Untabled)
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    function: AnnotatedFunction,
    per_call_costs: &impl FunctionCallCostProvider,
    is_tabled: FunctionTablingType,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    debug_assert!(all_calls_in_pipeline(function.stages.as_slice()).iter().all(|f| {
        per_call_costs.get_call_cost(f);
        true
    })); // Will crash
    let AnnotatedFunction { variable_registry, parameter_registry, arguments, stages, return_, .. } = function;
    let (argument_positions, executable_stages, _) = compile_pipeline_stages(
        statistics,
        &variable_registry,
        per_call_costs,
        stages,
        arguments.into_iter(),
        &return_.referenced_variables(),
    )?;

    let returns = compile_return_operation(&executable_stages, return_)?;
    let single_call_cost = Cost { cost: 1.0, io_ratio: 1.0 }; // TODO
    Ok(ExecutableFunction {
        executable_id: next_executable_id(),
        executable_stages,
        argument_positions,
        returns,
        parameter_registry: Arc::new(parameter_registry),
        is_tabled,
        single_call_cost,
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
