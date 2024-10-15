/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use concept::{thing::statistics::Statistics, type_::attribute_type::AttributeType};
use error::typedb_error;
use ir::{pattern::ParameterID, pipeline::VariableRegistry};

use crate::{
    annotation::fetch::{AnnotatedFetch, AnnotatedFetchListSubFetch, AnnotatedFetchObject, AnnotatedFetchSome},
    executable::{
        function::{compile_function, ExecutableFunction},
        match_::planner::function_plan::ExecutableFunctionRegistry,
        pipeline::{compile_pipeline, compile_stages_and_fetch, ExecutableStage},
        ExecutableCompilationError,
    },
    VariablePosition,
};

pub struct ExecutableFetch {
    object_instruction: FetchObjectInstruction,
}

enum FetchSomeInstruction {
    SingleVar(VariablePosition),
    SingleAttribute(VariablePosition, AttributeType<'static>),
    SingleFunction(ExecutableFunction),

    Object(Box<FetchObjectInstruction>),

    ListFunction(ExecutableFunction),
    ListSubFetch(ExecutableFetchListSubFetch),
    ListAttributesAsList(VariablePosition, AttributeType<'static>),
    ListAttributesFromList(VariablePosition, AttributeType<'static>),
}

enum FetchObjectInstruction {
    Entries(HashMap<ParameterID, FetchSomeInstruction>),
    Attributes(VariablePosition),
}

struct ExecutableFetchListSubFetch {
    stages: Vec<ExecutableStage>,
    fetch: ExecutableFetch,
}

pub fn compile_fetch(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    fetch: AnnotatedFetch,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<ExecutableFetch, FetchCompilationError> {
    let compiled = compile_object(statistics, available_functions, fetch.object, variable_positions)?;
    Ok(ExecutableFetch { object_instruction: compiled })
}

fn compile_object(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    fetch_object: AnnotatedFetchObject,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<FetchObjectInstruction, FetchCompilationError> {
    match fetch_object {
        AnnotatedFetchObject::Entries(entries) => {
            let mut compiled_entries = HashMap::with_capacity(entries.len());
            for (key, value) in entries {
                let compiled = compile_some(statistics, available_functions, value, variable_positions)?;
                compiled_entries.insert(key, compiled);
            }
            Ok(FetchObjectInstruction::Entries(compiled_entries))
        }
        AnnotatedFetchObject::Attributes(var) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok(FetchObjectInstruction::Attributes(*position))
        }
    }
}

fn compile_some(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    some: AnnotatedFetchSome,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<FetchSomeInstruction, FetchCompilationError> {
    match some {
        AnnotatedFetchSome::SingleVar(var) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok(FetchSomeInstruction::SingleVar(*position))
        }
        AnnotatedFetchSome::SingleAttribute(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok(FetchSomeInstruction::SingleAttribute(*position, attribute_type))
        }
        AnnotatedFetchSome::SingleFunction(function) => {
            let compiled = compile_function(statistics, available_functions, function)
                .map_err(|err| FetchCompilationError::AnonymousFunctionCompilation { typedb_source: Box::new(err) })?;
            Ok(FetchSomeInstruction::SingleFunction(compiled))
        }
        AnnotatedFetchSome::Object(object) => {
            let compiled = compile_object(statistics, available_functions, *object, variable_positions)?;
            Ok(FetchSomeInstruction::Object(Box::new(compiled)))
        }
        AnnotatedFetchSome::ListFunction(function) => {
            let compiled = compile_function(statistics, available_functions, function)
                .map_err(|err| FetchCompilationError::AnonymousFunctionCompilation { typedb_source: Box::new(err) })?;
            Ok(FetchSomeInstruction::ListFunction(compiled))
        }
        AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
            let AnnotatedFetchListSubFetch { variable_registry, input_variables, stages, fetch } = sub_fetch;
            let (compiled_stages, compiled_fetch) = compile_stages_and_fetch(
                statistics,
                Arc::new(variable_registry),
                available_functions,
                stages,
                Some(fetch),
                input_variables,
            )
            .map_err(|err| FetchCompilationError::SubFetchCompilation { typedb_source: Box::new(err) })?;

            Ok(FetchSomeInstruction::ListSubFetch(ExecutableFetchListSubFetch {
                stages: compiled_stages,
                fetch: compiled_fetch.unwrap(),
            }))
        }
        AnnotatedFetchSome::ListAttributesAsList(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok(FetchSomeInstruction::ListAttributesAsList(*position, attribute_type))
        }
        AnnotatedFetchSome::ListAttributesFromList(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok(FetchSomeInstruction::ListAttributesFromList(*position, attribute_type))
        }
    }
}

typedb_error!(
    pub FetchCompilationError(component = "Fetch compilation", prefix = "FEC") {
        FetchVariableNotFound(1, "Internal compilation error - fetch variable {var} not found in expected inputs", var: Variable),
        AnonymousFunctionCompilation(2, "Failed to compile inline/anonymous function.", ( typedb_source : Box<ExecutableCompilationError>)),
        SubFetchCompilation(3, "Failed to compile sub-fetch pipeline.", ( typedb_source : Box<ExecutableCompilationError>)),
    }
);
