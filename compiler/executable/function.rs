/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};
use std::collections::HashSet;
use typeql::schema::definable::function::SingleSelector;

use concept::thing::statistics::Statistics;
use ir::pipeline::VariableRegistry;

use crate::{
    annotation::function::AnnotatedFunction,
    executable::{
        ExecutableCompilationError,
        pipeline::{compile_pipeline_stages, ExecutableStage},
    },
    VariablePosition,
};
use crate::annotation::function::AnnotatedFunctionReturn;
use crate::executable::reduce::ReduceInstruction;

pub struct ExecutableFunction {
    executable_stages: Vec<ExecutableStage>,
    returns: ExecutableReturn,
}

pub enum ExecutableReturn {
    Stream(Vec<VariablePosition>),
    Single(SingleSelector, Vec<VariablePosition>),
    Check,
    Reduce(Vec<ReduceInstruction<VariablePosition>>)
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    function: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let AnnotatedFunction { variable_registry, arguments, stages, return_ } = function;
    let arguments_set = HashSet::from_iter(arguments.into_iter());
    let mut executable_stages = compile_pipeline_stages(statistics, Arc::new(variable_registry), stages, arguments_set)?;
    let returns = compile_return_operation(&mut executable_stages, return_)?;
    Ok(ExecutableFunction { executable_stages, returns })
}

fn compile_return_operation(
    executable_stages: &mut Vec<ExecutableStage>,
    return_: AnnotatedFunctionReturn,
) -> Result<ExecutableReturn, ExecutableCompilationError> {
    let variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());
    match return_ {
        AnnotatedFunctionReturn::Stream {variables, ..} => {
            Ok(ExecutableReturn::Stream(
                variables.iter().map(|var| variable_positions.get(var).unwrap().clone()).collect()
            ))
        }
        AnnotatedFunctionReturn::Single { selector, variables, .. } => {
            Ok(ExecutableReturn::Single(
                selector, 
                variables.iter().map(|var| variable_positions.get(var).unwrap().clone()).collect()
            ))
        }
        | AnnotatedFunctionReturn::ReduceCheck {} => {
            Ok(ExecutableReturn::Check)
        }
        | AnnotatedFunctionReturn::ReduceReducer { instructions } => {
            let positional_reducers = instructions.into_iter().map(|reducer| reducer.map(&variable_positions)).collect();
            Ok(ExecutableReturn::Reduce(positional_reducers))
        },
    }
}
