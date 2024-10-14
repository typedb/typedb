/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};
use std::collections::HashSet;

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

pub type ExecutableReturn = Vec<VariablePosition>; // TODO: in case we need to become an enum.
pub struct ExecutableFunction {
    executable_stages: Vec<ExecutableStage>,
    returns: ExecutableReturn,
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
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());
    match return_ {
        AnnotatedFunctionReturn::Stream {variables, ..} => {
            Ok(variables.iter().map(|var| stages_variable_positions.get(var).unwrap().clone()).collect())
        }
        AnnotatedFunctionReturn::Single {..}
        | AnnotatedFunctionReturn::ReduceCheck {}
        | AnnotatedFunctionReturn::ReduceReducer {..} => { 
            todo!() 
        },
    }
}
