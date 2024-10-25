/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::definition::definition_key::DefinitionKey,
    value::{label::Label, value_type::ValueType},
};
use ir::{
    pipeline::{
        function::{Function, FunctionBody, ReturnOperation},
        VariableRegistry,
    },
    translation::tokens::translate_value_type,
};
use itertools::Either;
use storage::snapshot::ReadableSnapshot;
use typeql::{schema::definable::function::SingleSelector, type_::NamedType, TypeRef, TypeRefAny};

use crate::{
    annotation::{
        expression::compiled_expression::ExpressionValueType,
        pipeline::{annotate_pipeline_stages, resolve_reducer_by_value_type, AnnotatedStage},
        type_seeder, FunctionAnnotationError,
    },
    executable::reduce::ReduceInstruction,
};

#[derive(Debug, Clone)]
pub enum FunctionParameterAnnotation {
    Concept(BTreeSet<Type>),
    Value(ValueType),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunction {
    pub variable_registry: VariableRegistry,
    pub arguments: Vec<Variable>,
    pub stages: Vec<AnnotatedStage>,
    pub return_: AnnotatedFunctionReturn,
}

#[derive(Debug, Clone)]
pub enum AnnotatedFunctionReturn {
    Stream { variables: Vec<Variable>, annotations: Vec<FunctionParameterAnnotation> },
    Single { selector: SingleSelector, variables: Vec<Variable>, annotations: Vec<FunctionParameterAnnotation> },
    ReduceCheck {},
    ReduceReducer { instructions: Vec<ReduceInstruction<Variable>> },
}

impl AnnotatedFunctionReturn {
    pub fn annotations(&self) -> Cow<'_, [FunctionParameterAnnotation]> {
        match self {
            AnnotatedFunctionReturn::Stream { annotations, .. } => Cow::Borrowed(annotations),
            AnnotatedFunctionReturn::Single { annotations, .. } => Cow::Borrowed(annotations),
            AnnotatedFunctionReturn::ReduceCheck { .. } => {
                Cow::Borrowed(&[FunctionParameterAnnotation::Value(ValueType::Boolean)])
            }
            AnnotatedFunctionReturn::ReduceReducer { instructions } => Cow::Owned(
                instructions
                    .iter()
                    .map(|instruction| FunctionParameterAnnotation::Value(instruction.output_type()))
                    .collect(),
            ),
        }
    }
}

/// Indexed by Function ID
#[derive(Debug)]
pub struct IndexedAnnotatedFunctions {
    functions: HashMap<DefinitionKey<'static>, AnnotatedFunction>,
}

impl IndexedAnnotatedFunctions {
    pub fn new(functions: HashMap<DefinitionKey<'static>, AnnotatedFunction>) -> Self {
        Self { functions }
    }

    pub fn empty() -> Self {
        Self { functions: HashMap::new() }
    }
}

pub trait AnnotatedFunctions {
    type ID;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction>;
    fn iter_functions(&self) -> impl Iterator<Item = (Self::ID, &AnnotatedFunction)>;

    fn is_empty(&self) -> bool;
}

impl AnnotatedFunctions for IndexedAnnotatedFunctions {
    type ID = DefinitionKey<'static>;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.functions.get(&id)
    }

    fn iter_functions(&self) -> impl Iterator<Item = (DefinitionKey<'static>, &AnnotatedFunction)> {
        self.functions.iter().map(|(key, function)| (key.clone(), function))
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

    pub fn into_iter_functions(self) -> impl Iterator<Item = AnnotatedFunction> {
        self.unindexed_functions.into_iter()
    }
}

impl AnnotatedFunctions for AnnotatedUnindexedFunctions {
    type ID = usize;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.unindexed_functions.get(id)
    }

    fn iter_functions(&self) -> impl Iterator<Item = (usize, &AnnotatedFunction)> {
        self.unindexed_functions.iter().enumerate()
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
) -> Result<AnnotatedUnindexedFunctions, FunctionAnnotationError> {
    // In the preliminary annotations, functions are annotated based only on the variable categories of the called function.
    let preliminary_annotated_functions = functions
        .iter_mut()
        .map(|function| annotate_named_function(function, snapshot, type_manager, indexed_annotated_functions, None))
        .collect::<Result<Vec<AnnotatedFunction>, FunctionAnnotationError>>()?;
    let preliminary_annotations = AnnotatedUnindexedFunctions::new(preliminary_annotated_functions);

    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotated_functions = functions
        .iter_mut()
        .map(|function| {
            annotate_named_function(
                function,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                Some(&preliminary_annotations),
            )
        })
        .collect::<Result<Vec<AnnotatedFunction>, FunctionAnnotationError>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(AnnotatedUnindexedFunctions::new(annotated_functions))
}

pub(crate) fn annotate_anonymous_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
    caller_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    caller_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, FunctionAnnotationError> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_none());
    let mut argument_concept_variable_types = BTreeMap::new();
    let mut argument_value_variable_types = BTreeMap::new();
    for var in arguments {
        if let Some(concept_annotation) = caller_type_annotations.get(var) {
            argument_concept_variable_types.insert(*var, concept_annotation.clone());
        } else if let Some(value_annotation) = caller_value_type_annotations.get(var) {
            argument_value_variable_types.insert(*var, value_annotation.clone());
        } else {
            todo!("Throw error")
        }
    }

    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        indexed_annotated_functions,
        local_functions,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

pub(super) fn annotate_named_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFunction, FunctionAnnotationError> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_some());
    let mut argument_concept_variable_types = BTreeMap::new();
    let mut argument_value_variable_types = BTreeMap::new();
    for (arg_index, (var, label)) in zip(arguments, argument_labels.as_ref().unwrap()).enumerate() {
        match get_argument_annotations_from_labels(snapshot, type_manager, label, arg_index)? {
            Either::Left(concept_annotation) => {
                argument_concept_variable_types.insert(*var, concept_annotation);
            }
            Either::Right(value_annotation) => {
                argument_value_variable_types.insert(*var, value_annotation);
            }
        }
    }
    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        indexed_annotated_functions,
        local_functions,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

fn annotate_function_impl(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
    argument_concept_variable_types: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    argument_value_variable_types: BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, FunctionAnnotationError> {
    let Function { name, context, function_body: FunctionBody { stages, return_operation }, arguments, .. } = function;

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
    .map_err(|err| FunctionAnnotationError::TypeInference { name: name.to_string(), typedb_source: Box::new(err) })?;

    let return_ = annotate_return(
        snapshot,
        type_manager,
        &context.variable_registry,
        return_operation,
        &running_variable_types,
        &running_value_types,
    )?;
    Ok(AnnotatedFunction {
        variable_registry: context.variable_registry.clone(),
        arguments: arguments.clone(),
        stages,
        return_,
    })
}

fn annotate_arguments_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    arguments: &[Variable],
    argument_labels: &[TypeRefAny],
) -> Result<(BTreeMap<Variable, Arc<BTreeSet<Type>>>, BTreeMap<Variable, ExpressionValueType>), FunctionAnnotationError>
{
    // TODO
    let mut variable_types = BTreeMap::new();
    let mut value_types = BTreeMap::new();
    for (index, var) in arguments.iter().enumerate() {
        match get_argument_annotations_from_labels(snapshot, type_manager, &argument_labels[index], index)? {
            Either::Left(types) => {
                variable_types.insert(*var, types);
            }
            Either::Right(value_type) => {
                value_types.insert(*var, value_type);
            }
        }
    }
    Ok((variable_types, value_types))
}

fn get_argument_annotations_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    typeql_label: &TypeRefAny,
    arg_index: usize,
) -> Result<Either<Arc<BTreeSet<Type>>, ExpressionValueType>, FunctionAnnotationError> {
    let TypeRef::Named(inner_type) = (match typeql_label {
        TypeRefAny::Type(inner) => inner,
        TypeRefAny::Optional(typeql::type_::Optional { inner: _inner, .. }) => todo!(),
        TypeRefAny::List(typeql::type_::List { inner: _inner, .. }) => todo!(),
    }) else {
        unreachable!("Function argument labels cannot be variable.");
    };
    match inner_type {
        NamedType::Label(label) => {
            // TODO: could be a struct value type in the future!
            let types = type_seeder::get_type_annotation_and_subtypes_from_label(
                snapshot,
                type_manager,
                &Label::build(label.ident.as_str()),
            )
            .map_err(|source| FunctionAnnotationError::CouldNotResolveArgumentType { index: arg_index, source })?;
            Ok(Either::Left(Arc::new(types)))
        }
        NamedType::BuiltinValueType(value_type) => {
            // TODO: This may be list
            let value = ExpressionValueType::Single(translate_value_type(&value_type.token));
            Ok(Either::Right(value))
        }
        NamedType::Role(_) => unreachable!("A function argument label was wrongly parsed as role-type."),
    }
}

fn annotate_return(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
    return_operation: &ReturnOperation,
    input_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunctionReturn, FunctionAnnotationError> {
    // TODO: We don't consider the user annotations.
    match return_operation {
        ReturnOperation::Stream(vars) => {
            let type_annotations = vars
                .iter()
                .map(|var| get_function_parameter(var, input_type_annotations, input_value_type_annotations))
                .collect();
            Ok(AnnotatedFunctionReturn::Stream { variables: vars.clone(), annotations: type_annotations })
        }
        ReturnOperation::Single(selector, vars) => {
            let type_annotations = vars
                .iter()
                .map(|var| get_function_parameter(var, input_type_annotations, input_value_type_annotations))
                .collect();
            Ok(AnnotatedFunctionReturn::Single {
                selector: selector.clone(),
                variables: vars.clone(),
                annotations: type_annotations,
            })
        }
        ReturnOperation::ReduceReducer(reducers) => {
            let mut instructions = Vec::with_capacity(reducers.len());
            for &reducer in reducers {
                let instruction = resolve_reducer_by_value_type(
                    snapshot,
                    type_manager,
                    variable_registry,
                    reducer,
                    input_type_annotations,
                    input_value_type_annotations,
                )
                .map_err(|err| FunctionAnnotationError::ReturnReduce { typedb_source: Box::new(err) })?;
                instructions.push(instruction);
            }
            Ok(AnnotatedFunctionReturn::ReduceReducer { instructions })
        }
        ReturnOperation::ReduceCheck() => Ok(AnnotatedFunctionReturn::ReduceCheck {}),
    }
}

fn get_function_parameter(
    variable: &Variable,
    body_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    body_variable_value_types: &BTreeMap<Variable, ExpressionValueType>,
) -> FunctionParameterAnnotation {
    if let Some(arced_types) = body_variable_annotations.get(variable) {
        let types: &BTreeSet<Type> = arced_types;
        FunctionParameterAnnotation::Concept(types.clone())
    } else if let Some(expression_value_type) = body_variable_value_types.get(variable) {
        FunctionParameterAnnotation::Value(expression_value_type.value_type().clone())
    } else {
        unreachable!("Could not find annotations for a function return variable.")
    }
}
