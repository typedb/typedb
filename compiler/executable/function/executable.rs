/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pipeline::{
    function_signature::{FunctionID, FunctionIDAPI},
    ParameterRegistry,
};
use typeql::schema::definable::function::SingleSelector;

use crate::{
    annotation::function::{AnnotatedFunction, AnnotatedFunctionReturn},
    executable::{
        function::{
            recursion_analyser::{all_calls_in_pipeline, determine_compilation_order_and_tabling_types},
            ExecutableFunctionRegistry, FunctionCallCostProvider, FunctionTablingType,
        },
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
    pub tabling_type: FunctionTablingType,
    pub parameter_registry: Arc<ParameterRegistry>,
    pub(crate) single_call_cost: Cost,
}

#[derive(Debug, Clone)]
pub enum ExecutableReturn {
    Stream(Vec<VariablePosition>),
    Single(SingleSelector, Vec<VariablePosition>),
    Check,
    Reduce(Arc<ReduceRowsExecutable>),
}

pub(crate) fn compile_single_untabled_function(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    to_compile: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    compile_function(statistics, to_compile, cached_plans, FunctionTablingType::Untabled)
}

pub(crate) fn compile_functions<FIDType: FunctionIDAPI>(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    mut to_compile: HashMap<FIDType, AnnotatedFunction>,
) -> Result<HashMap<FIDType, ExecutableFunction>, ExecutableCompilationError> {
    // TODO: Cache compiled schema functions?
    let (post_order, tabling_types) = determine_compilation_order_and_tabling_types(&to_compile)?;
    let mut context = FunctionCompilationContext::new(cached_plans, tabling_types);

    // Compiling functions in post-order ensures dependencies are compiled first, and we have a cost available.
    for fid in post_order {
        debug_assert!(to_compile.contains_key(&fid)); // occurs exactly-once in post_order
        if let Some(function) = to_compile.remove(&fid) {
            let tabling_type = context.tabling_types.get(&fid).unwrap().clone();
            let compiled_function = compile_function(statistics, function, &context, tabling_type)?;
            context.compiled.insert(fid.clone(), compiled_function);
        }
    }
    debug_assert!(to_compile.is_empty());
    Ok(context.compiled)
}

fn compile_function(
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
        &stages,
        arguments.into_iter(),
        Some(&return_.referenced_variables()),
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
        tabling_type: is_tabled,
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

// Private
struct FunctionCompilationContext<'a, FIDType: FunctionIDAPI> {
    precompiled: &'a ExecutableFunctionRegistry,
    compiled: HashMap<FIDType, ExecutableFunction>,

    tabling_types: HashMap<FIDType, FunctionTablingType>,
}

impl<'a, FIDType: FunctionIDAPI> FunctionCompilationContext<'a, FIDType> {
    fn new(cached_plans: &'a ExecutableFunctionRegistry, tabling_types: HashMap<FIDType, FunctionTablingType>) -> Self {
        FunctionCompilationContext { precompiled: cached_plans, compiled: HashMap::new(), tabling_types }
    }

    pub(crate) fn get_executable_function(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        if let Some(plan) = self.precompiled.get(function_id) {
            Some(plan)
        } else if let Ok(key) = &FIDType::try_from(function_id.clone()) {
            self.compiled.get(key)
        } else {
            None
        }
    }

    fn cycle_breaking_cost(&self) -> Cost {
        Cost { cost: 1.0, io_ratio: 1.0 } // TODO: Improve. This should simulate depth 1 recursion.
    }
}

impl<FIDType: FunctionIDAPI> FunctionCallCostProvider for FunctionCompilationContext<'_, FIDType> {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        if let Some(function) = self.get_executable_function(function_id) {
            function.single_call_cost
        } else {
            debug_assert!(matches!(
                FIDType::try_from(function_id.clone())
                    .map(|id| matches!(self.tabling_types.get(&id), Some(FunctionTablingType::Tabled(_)))),
                Ok(true)
            ));
            self.cycle_breaking_cost()
        }
    }
}
