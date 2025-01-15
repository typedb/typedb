/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use encoding::graph::definition::definition_key::DefinitionKey;
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
        match_::planner::vertex::Cost,
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
    pub single_call_cost: Cost,
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

#[derive(Clone)]
pub struct ExecutableFunctionRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
    preamble_functions: HashMap<usize, ExecutableFunction>,
}

impl fmt::Debug for ExecutableFunctionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutableFunctionRegistry { omitted }")
    }
}

impl ExecutableFunctionRegistry {
    pub(crate) fn new(
        schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
        preamble_functions: HashMap<usize, ExecutableFunction>,
    ) -> Self {
        Self { schema_functions, preamble_functions }
    }

    pub fn empty() -> Self {
        Self::new(Arc::new(HashMap::new()), HashMap::new())
    }

    pub fn get(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        match function_id {
            FunctionID::Schema(id) => self.schema_functions.get(id),
            FunctionID::Preamble(id) => self.preamble_functions.get(id),
        }
    }

    pub(crate) fn schema_functions(&self) -> Arc<HashMap<DefinitionKey, ExecutableFunction>> {
        self.schema_functions.clone()
    }
}

impl FunctionCallCostProvider for ExecutableFunctionRegistry {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        self.get(function_id).unwrap().single_call_cost
    }
}

struct FunctionCompiler<'a, FIDType: FunctionIDAPI> {
    precompiled: &'a ExecutableFunctionRegistry,
    to_compile: HashMap<FIDType, AnnotatedFunction>,
    compiled: HashMap<FIDType, ExecutableFunction>,
    must_table: HashSet<FIDType>,
}

impl<'a, FIDType: FunctionIDAPI> FunctionCompiler<'a, FIDType> {
    fn new(cached_plans: &'a ExecutableFunctionRegistry, to_compile: HashMap<FIDType, AnnotatedFunction>) -> Self {
        FunctionCompiler { precompiled: cached_plans, to_compile, compiled: HashMap::new(), must_table: HashSet::new() }
    }

    pub(crate) fn get_executable_function(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        if let Some(plan) = self.precompiled.get(function_id) {
            Some(plan)
        } else if let Ok(key) = &FIDType::try_from(function_id.clone()) {
            self.compiled.get(&key)
        } else {
            None
        }
    }

    fn cycle_breaking_cost(&self) -> Cost {
        Cost { cost: 1.0, io_ratio: 1.0 } // TODO: Improve. This should simulate depth 1 recursion.
    }
}

impl<'a, FIDType: FunctionIDAPI> FunctionCallCostProvider for FunctionCompiler<'a, FIDType> {
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
    let mut planner = FunctionCompiler::new(cached_plans, to_compile);
    let mut cycle_detection = HashSet::new();
    while !planner.to_compile.is_empty() {
        let id = planner.to_compile.keys().find_or_first(|_| true).unwrap().clone();
        compile_functions_impl(statistics, &mut planner, &mut cycle_detection, id)?;
    }

    Ok(planner.compiled)
}

fn compile_functions_impl<'a, FIDType: FunctionIDAPI>(
    statistics: &Statistics,
    planner: &mut FunctionCompiler<'a, FIDType>,
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
    planner.compiled.insert(current.clone(), compiled_function);
    Ok(())
}

pub(crate) fn compile_single_untabled_function(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    to_compile: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let planner = FunctionCompiler::new(cached_plans, HashMap::<usize, _>::new());
    compile_function(statistics, to_compile, &planner, FunctionTablingType::Untabled)
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    function: AnnotatedFunction,
    call_cost_provider: &impl FunctionCallCostProvider,
    is_tabled: FunctionTablingType,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    debug_assert!(all_calls_in_pipeline(function.stages.as_slice()).iter().all(|f| {
        call_cost_provider.get_call_cost(f);
        true // The call above will crash if the assertion fails.
    }));
    let AnnotatedFunction { variable_registry, parameter_registry, arguments, stages, return_, .. } = function;
    let (argument_positions, executable_stages, _) = compile_pipeline_stages(
        statistics,
        &variable_registry,
        call_cost_provider,
        stages,
        arguments.into_iter(),
        &return_.referenced_variables(),
    )?;

    let returns = compile_return_operation(&executable_stages, return_)?;
    debug_assert!(executable_stages.iter().any(|stage| matches!(stage, ExecutableStage::Match(_))));
    let single_call_cost =
        executable_stages
            .iter()
            .filter_map(|stage| {
                if let ExecutableStage::Match(m) = stage {
                    Some(m.planner_statistics().query_cost)
                } else {
                    None
                }
            })
            .reduce(|x, y| x.chain(y))
            .unwrap();
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
