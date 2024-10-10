/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::{graph::definition::definition_key::DefinitionKey, value::value_type::ValueType};
use ir::{
    pattern::Vertex,
    pipeline::{
        function::{AnonymousFunction, Function, FunctionBody, ReturnOperation},
        function_signature::FunctionIDAPI,
    },
};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    pipeline::{annotate_pipeline, annotate_pipeline_stages, AnnotatedStage},
    FunctionTypeInferenceError,
};

#[derive(Debug, Clone)]
pub enum FunctionParameterAnnotation {
    Concept(BTreeSet<Type>),
    Value(ValueType),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunction {
    pub stages: Vec<AnnotatedStage>,
    pub return_operation: ReturnOperation,
    pub return_annotations: Vec<FunctionParameterAnnotation>,
}

#[derive(Debug, Clone)]
pub struct AnnotatedAnonymousFunction {
    stages: Vec<AnnotatedStage>,
    return_annotations: Vec<FunctionParameterAnnotation>,
}

/// Indexed by Function ID
#[derive(Debug)]
pub struct IndexedAnnotatedFunctions {
    functions: Vec<AnnotatedFunction>,
}

impl IndexedAnnotatedFunctions {
    pub fn new(functions: Vec<AnnotatedFunction>) -> Self {
        Self { functions }
    }

    pub fn empty() -> Self {
        Self { functions: Vec::new() }
    }
}

pub trait AnnotatedFunctions {
    type ID;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction>;

    fn is_empty(&self) -> bool;
}

impl AnnotatedFunctions for IndexedAnnotatedFunctions {
    type ID = DefinitionKey<'static>;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.functions.get(id.as_usize())
    }

    fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

// May hold IR & Annotations for either uncommitted Schema functions or Preamble functions
// For schema functions, The index does not correspond to function_id.as_usize().
pub struct AnnotatedUnindexedFunctions {
    unindexed_functions: Vec<AnnotatedFunction>,
}

impl AnnotatedUnindexedFunctions {
    pub fn new(functions: Vec<AnnotatedFunction>) -> Self {
        Self { unindexed_functions: functions }
    }

    pub fn empty() -> Self {
        Self { unindexed_functions: Vec::new() }
    }

    pub fn iter_functions(&self) -> impl Iterator<Item = &AnnotatedFunction> {
        self.unindexed_functions.iter()
    }

    pub fn into_iter_functions(self) -> impl Iterator<Item = AnnotatedFunction> {
        self.unindexed_functions.into_iter()
    }
}

impl AnnotatedFunctions for AnnotatedUnindexedFunctions {
    type ID = usize;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.unindexed_functions.get(id)
    }

    fn is_empty(&self) -> bool {
        self.unindexed_functions.is_empty()
    }
}

pub fn annotate_functions(
    mut functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
) -> Result<AnnotatedUnindexedFunctions, FunctionTypeInferenceError> {
    // In the preliminary annotations, functions are annotated based only on the variable categories of the called function.
    let preliminary_annotated_functions = functions
        .iter_mut()
        .map(|function| annotate_function(function, snapshot, type_manager, indexed_annotated_functions, None))
        .collect::<Result<Vec<AnnotatedFunction>, FunctionTypeInferenceError>>()?;
    let preliminary_annotations = AnnotatedUnindexedFunctions::new(preliminary_annotated_functions);

    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotated_functions = functions
        .iter_mut()
        .map(|function| {
            annotate_function(
                function,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                Some(&preliminary_annotations),
            )
        })
        .collect::<Result<Vec<AnnotatedFunction>, FunctionTypeInferenceError>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(AnnotatedUnindexedFunctions::new(annotated_functions))
}

pub fn annotate_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFunction, FunctionTypeInferenceError> {
    let Function { name, context, function_body: FunctionBody { stages, return_operation }, arguments } = function;
    // TODO: Work the argument in.
    let (argument_concept_variable_types, argument_value_variable_types) =
        annotate_arguments(arguments, snapshot, type_manager)?;

    let (stages, running_variable_types, running_value_types) = annotate_pipeline_stages(
        snapshot,
        type_manager,
        indexed_annotated_functions,
        &mut context.variable_registry,
        &context.parameters,
        local_functions,
        stages.clone(),
        argument_concept_variable_types,
        argument_value_variable_types,
    )
    .map_err(|err| FunctionTypeInferenceError::TypeInference {
        name: name.to_string(),
        typedb_source: Box::new(err),
    })?;
    let return_annotations =
        extract_return_type_annotations(return_operation, &running_variable_types, &running_value_types);
    Ok(AnnotatedFunction { stages, return_annotations, return_operation: return_operation.clone() })
}

fn annotate_arguments(
    function_arguments: &mut Vec<Variable>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<(BTreeMap<Variable, Arc<BTreeSet<Type>>>, BTreeMap<Variable, ValueType>), FunctionTypeInferenceError> {
    // TODO
    Ok((BTreeMap::new(), BTreeMap::new()))
}

pub fn extract_return_type_annotations(
    return_operation: &ReturnOperation,
    body_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    body_variable_value_types: &BTreeMap<Variable, ValueType>,
) -> Vec<FunctionParameterAnnotation> {
    match return_operation {
        ReturnOperation::Stream(vars) => {
            vars.iter()
                .map(|var| {
                    get_function_parameter(var, body_variable_annotations, body_variable_value_types)
                    // TODO: body_variable_value_types
                })
                .collect()
        }
        ReturnOperation::Single(_, vars) => vars
            .iter()
            .map(|var| get_function_parameter(var, body_variable_annotations, body_variable_value_types))
            .collect(),
        ReturnOperation::ReduceReducer(reducers) => {
            // aggregates return value types?
            todo!()
        }
        ReturnOperation::ReduceCheck() => {
            // aggregates return value types?
            todo!()
        }
    }
}

fn get_function_parameter(
    variable: &Variable,
    body_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    body_variable_value_types: &BTreeMap<Variable, ValueType>,
) -> FunctionParameterAnnotation {
    if let Some(arced_types) = body_variable_annotations.get(variable) {
        let types: &BTreeSet<Type> = &arced_types;
        FunctionParameterAnnotation::Concept(types.clone())
    } else if let Some(value_type) = body_variable_value_types.get(variable) {
        FunctionParameterAnnotation::Value(value_type.clone())
    } else {
        unreachable!()
    }
}

pub fn annotate_anonymous_function(
    function: &AnonymousFunction,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedAnonymousFunction, FunctionTypeInferenceError> {
    // let root_graph = infer_types(
    //     snapshot,
    //     function.block(),
    //     function.variable_registry(),
    //     type_manager,
    //     &BTreeMap::new(),
    //     indexed_annotated_functions,
    //     local_functions,
    // )
    // .map_err(|err| FunctionTypeInferenceError::TypeInference {
    //     name: function.name().to_string(),
    //     typedb_source: err,
    // })?;
    // let body_annotations = TypeAnnotations::build(root_graph);
    // let return_types = function.return_operation().return_types(body_annotations.vertex_annotations());
    // Ok(FunctionAnnotations { return_annotations: return_types, block_annotations: body_annotations })
    todo!("We need to allow a function to contain an entire pipeline, instead of just a match block")
}
