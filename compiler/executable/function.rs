/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use typeql::schema::definable::function::SingleSelector;

use crate::{
    annotation::function::{AnnotatedFunction, AnnotatedFunctionReturn},
    executable::{
        match_::planner::function_plan::ExecutableFunctionRegistry,
        pipeline::{compile_pipeline_stages, ExecutableStage},
        reduce::ReduceInstruction,
        ExecutableCompilationError,
    },
    VariablePosition,
};

pub struct ExecutableFunction {
    executable_stages: Vec<ExecutableStage>,
    argument_positions: HashMap<Variable, VariablePosition>,
    returns: ExecutableReturn,
    // is_tabled: bool, // TODO: Essential
    // plan_cost: f64, // TODO: Where do we fit this in?
}

pub enum ExecutableReturn {
    Stream(Vec<VariablePosition>),
    Single(SingleSelector, Vec<VariablePosition>),
    Check,
    Reduce(Vec<ReduceInstruction<VariablePosition>>),
}

// TODO: Don't bother maintaining this function. It'll have to disappear into the planner.
pub(crate) fn compile_function(
    statistics: &Statistics,
    schema_functions: &ExecutableFunctionRegistry, // Can't have preamble in them when you're compiling functions
    function: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let AnnotatedFunction { variable_registry, arguments, stages, return_ } = function;
    let (argument_positions, executable_stages) = compile_pipeline_stages(
        statistics,
        Arc::new(variable_registry),
        schema_functions,
        stages,
        arguments.into_iter(),
    )?;
    let returns = compile_return_operation(&executable_stages, return_)?;
    Ok(ExecutableFunction { executable_stages, argument_positions, returns })
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
            let positional_reducers =
                instructions.into_iter().map(|reducer| reducer.map(&variable_positions)).collect();
            Ok(ExecutableReturn::Reduce(positional_reducers))
        }
    }
}
