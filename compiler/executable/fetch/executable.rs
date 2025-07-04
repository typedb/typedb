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
        function::{
            executable::{compile_single_untabled_function, ExecutableFunction},
            ExecutableFunctionRegistry,
        },
        next_executable_id,
        pipeline::{compile_stages_and_fetch, ExecutableStage, TypePopulations},
        ExecutableCompilationError,
    },
    VariablePosition,
};

#[derive(Debug)]
pub struct ExecutableFetch {
    pub executable_id: u64,
    pub object_instruction: FetchObjectInstruction,
}

impl ExecutableFetch {
    fn new(object_instruction: FetchObjectInstruction) -> Self {
        Self { executable_id: next_executable_id(), object_instruction }
    }
}

#[derive(Debug)]
pub enum FetchSomeInstruction {
    SingleVar(VariablePosition),
    SingleAttribute(VariablePosition, AttributeType),
    SingleFunction(ExecutableFunction, HashMap<Variable, VariablePosition>),

    Object(Box<FetchObjectInstruction>),

    ListFunction(ExecutableFunction, HashMap<Variable, VariablePosition>),
    ListSubFetch(ExecutableFetchListSubFetch),
    ListAttributesAsList(VariablePosition, AttributeType),
    ListAttributesFromList(VariablePosition, AttributeType),
}

#[derive(Debug)]
pub enum FetchObjectInstruction {
    Entries(HashMap<ParameterID, FetchSomeInstruction>),
    Attributes(VariablePosition),
}

#[derive(Debug)]
pub struct ExecutableFetchListSubFetch {
    pub variable_registry: Arc<VariableRegistry>,
    pub input_position_mapping: HashMap<VariablePosition, VariablePosition>,
    pub stages: Vec<ExecutableStage>,
    pub fetch: Arc<ExecutableFetch>,
}

pub fn compile_fetch(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    fetch: AnnotatedFetch,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<(ExecutableFetch, TypePopulations), FetchCompilationError> {
    let (compiled, type_populations) =
        compile_object(statistics, available_functions, fetch.object, variable_positions)?;
    Ok((ExecutableFetch::new(compiled), type_populations))
}

fn compile_object(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    fetch_object: AnnotatedFetchObject,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<(FetchObjectInstruction, TypePopulations), FetchCompilationError> {
    match fetch_object {
        AnnotatedFetchObject::Entries(entries) => {
            let mut compiled_entries = HashMap::with_capacity(entries.len());
            let mut type_populations = TypePopulations::default();
            for (key, value) in entries {
                let (compiled, pop) = compile_some(statistics, available_functions, value, variable_positions)?;
                compiled_entries.insert(key, compiled);
                type_populations.extend(pop);
            }
            Ok((FetchObjectInstruction::Entries(compiled_entries), type_populations))
        }
        AnnotatedFetchObject::Attributes(var) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok((FetchObjectInstruction::Attributes(*position), TypePopulations::default()))
        }
    }
}

fn compile_some(
    statistics: &Statistics,
    available_functions: &ExecutableFunctionRegistry,
    some: AnnotatedFetchSome,
    variable_positions: &HashMap<Variable, VariablePosition>,
) -> Result<(FetchSomeInstruction, TypePopulations), FetchCompilationError> {
    match some {
        AnnotatedFetchSome::SingleVar(var) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok((FetchSomeInstruction::SingleVar(*position), TypePopulations::default()))
        }
        AnnotatedFetchSome::SingleAttribute(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok((FetchSomeInstruction::SingleAttribute(*position, attribute_type), TypePopulations::default()))
        }
        AnnotatedFetchSome::SingleFunction(function) => {
            let compiled = compile_single_untabled_function(statistics, available_functions, function)
                .map_err(|err| FetchCompilationError::AnonymousFunctionCompilation { typedb_source: Box::new(err) })?;
            Ok((FetchSomeInstruction::SingleFunction(compiled, variable_positions.clone()), TypePopulations::default()))
        }
        AnnotatedFetchSome::Object(object) => {
            let (compiled, type_populations) =
                compile_object(statistics, available_functions, *object, variable_positions)?;
            Ok((FetchSomeInstruction::Object(Box::new(compiled)), type_populations))
        }
        AnnotatedFetchSome::ListFunction(function) => {
            let compiled = compile_single_untabled_function(statistics, available_functions, function)
                .map_err(|err| FetchCompilationError::AnonymousFunctionCompilation { typedb_source: Box::new(err) })?;
            Ok((FetchSomeInstruction::ListFunction(compiled, variable_positions.clone()), TypePopulations::default()))
        }
        AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
            let AnnotatedFetchListSubFetch { variable_registry, input_variables, stages, fetch } = sub_fetch;
            let (input_positions, compiled_stages, compiled_fetch, type_populations) = compile_stages_and_fetch(
                statistics,
                &variable_registry,
                available_functions,
                &stages,
                Some(fetch),
                &input_variables,
            )
            .map_err(|err| FetchCompilationError::SubFetchCompilation { typedb_source: Box::new(err) })?;
            let input_position_remapping = input_variables
                .into_iter()
                .map(|var| {
                    let parent_position = variable_positions.get(&var).unwrap();
                    let child_position = input_positions.get(&var).unwrap();
                    (*parent_position, *child_position)
                })
                .collect();

            Ok((
                FetchSomeInstruction::ListSubFetch(ExecutableFetchListSubFetch {
                    variable_registry: Arc::new(variable_registry),
                    input_position_mapping: input_position_remapping,
                    stages: compiled_stages,
                    fetch: compiled_fetch.unwrap(),
                }),
                type_populations,
            ))
        }
        AnnotatedFetchSome::ListAttributesAsList(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok((FetchSomeInstruction::ListAttributesAsList(*position, attribute_type), TypePopulations::default()))
        }
        AnnotatedFetchSome::ListAttributesFromList(var, attribute_type) => {
            let Some(position) = variable_positions.get(&var) else {
                return Err(FetchCompilationError::FetchVariableNotFound { var });
            };
            Ok((FetchSomeInstruction::ListAttributesFromList(*position, attribute_type), TypePopulations::default()))
        }
    }
}

typedb_error!(
    pub FetchCompilationError(component = "Fetch compiler", prefix = "FCP") {
        FetchVariableNotFound(1, "Internal compilation error - fetch variable {var} not found in expected inputs", var: Variable),
        AnonymousFunctionCompilation(2, "Failed to compile inline/anonymous function.", typedb_source: Box<ExecutableCompilationError>),
        SubFetchCompilation(3, "Failed to compile sub-fetch pipeline.", typedb_source: Box<ExecutableCompilationError>),
    }
);
