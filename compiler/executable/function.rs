/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pipeline::VariableRegistry;

use crate::{
    annotation::function::AnnotatedFunction,
    executable::{pipeline::ExecutablePipeline, ExecutableCompilationError},
    VariablePosition,
};

pub struct ExecutableFunction {
    executable: ExecutablePipeline,
    returns: HashMap<Variable, VariablePosition>,
}

pub(crate) fn compile_function(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    function: &AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    todo!()
}
