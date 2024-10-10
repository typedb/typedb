/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use concept::thing::statistics::Statistics;
use ir::pipeline::{function::ReturnOperation, VariableRegistry};

use crate::{
    annotation::function::AnnotatedFunction,
    executable::{
        pipeline::{compile_pipeline_stages, ExecutableStage},
        ExecutableCompilationError,
    },
    VariablePosition,
};

pub type ExecutableReturn = Vec<VariablePosition>; // TODO: in case we need to become an enum.
pub struct ExecutableFunction {
    executable_stages: Vec<ExecutableStage>,
    returns: ExecutableReturn,
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    function: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    let AnnotatedFunction { stages, return_operation, .. } = function;
    let mut executable_stages = compile_pipeline_stages(statistics, variable_registry.clone(), stages)?;
    let returns = compile_return_operation(&mut executable_stages, return_operation)?;
    Ok(ExecutableFunction { executable_stages, returns })
}

fn compile_return_operation(
    executable_stages: &mut Vec<ExecutableStage>,
    return_operation: ReturnOperation,
) -> Result<ExecutableReturn, ExecutableCompilationError> {
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());
    match return_operation {
        ReturnOperation::Stream(vars) => {
            Ok(vars.iter().map(|var| stages_variable_positions.get(var).unwrap().clone()).collect())
        }
        ReturnOperation::Single(_, _) | ReturnOperation::ReduceCheck() | ReturnOperation::ReduceReducer(_) => todo!(),
    }
}
