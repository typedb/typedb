/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
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
        function_signature::FunctionID,
        ParameterRegistry, VariableRegistry,
    },
    translation::tokens::translate_value_type,
};
use storage::snapshot::ReadableSnapshot;
use typeql::{
    schema::definable::function::{Output, SingleSelector},
    type_::NamedType,
    TypeRef, TypeRefAny,
};

use crate::{
    annotation::{
        expression::compiled_expression::ExpressionValueType,
        pipeline::{annotate_pipeline_stages, resolve_reducer_by_value_type, AnnotatedStage},
        type_seeder, FunctionAnnotationError, TypeInferenceError,
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
    pub parameter_registry: ParameterRegistry,
    pub arguments: Vec<Variable>,
    pub return_: AnnotatedFunctionReturn,
    pub stages: Vec<AnnotatedStage>,
    pub annotated_signature: AnnotatedFunctionSignature,
}

// TODO: Merge with ReturnOperation
#[derive(Debug, Clone)]
pub enum AnnotatedFunctionReturn {
    Stream { variables: Vec<Variable> },
    Single { selector: SingleSelector, variables: Vec<Variable> },
    ReduceCheck {},
    ReduceReducer { instructions: Vec<ReduceInstruction<Variable>> },
}

impl AnnotatedFunctionReturn {
    pub(crate) fn referenced_variables(&self) -> Vec<Variable> {
        match self {
            AnnotatedFunctionReturn::Stream { variables, .. } => variables.clone(),
            AnnotatedFunctionReturn::Single { variables, .. } => variables.clone(),
            AnnotatedFunctionReturn::ReduceCheck { .. } => Vec::new(),
            AnnotatedFunctionReturn::ReduceReducer { instructions } => {
                instructions.iter().filter_map(|x| x.id()).collect()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunctionSignature {
    pub arguments: Vec<FunctionParameterAnnotation>,
    pub returned: Vec<FunctionParameterAnnotation>,
}

pub type AnnotatedPreambleFunctions = Vec<AnnotatedFunction>;
pub type AnnotatedSchemaFunctions = HashMap<DefinitionKey, AnnotatedFunction>;

trait GetAnnotatedSignature {
    fn get_annotated_signature(&self) -> &AnnotatedFunctionSignature;
}

impl GetAnnotatedSignature for AnnotatedFunctionSignature {
    fn get_annotated_signature(&self) -> &AnnotatedFunctionSignature {
        self
    }
}

impl GetAnnotatedSignature for AnnotatedFunction {
    fn get_annotated_signature(&self) -> &AnnotatedFunctionSignature {
        &self.annotated_signature
    }
}

pub trait AnnotatedFunctionSignatures {
    fn get_annotated_signature(&self, function_id: &FunctionID) -> Option<&AnnotatedFunctionSignature>;
}

#[derive(Debug)]
pub struct AnnotatedFunctionSignaturesImpl<'a, T1: GetAnnotatedSignature, T2: GetAnnotatedSignature> {
    schema_functions: &'a HashMap<DefinitionKey, T1>,
    local_functions: &'a Vec<T2>,
}

impl<'a, T1: GetAnnotatedSignature, T2: GetAnnotatedSignature> AnnotatedFunctionSignaturesImpl<'a, T1, T2> {
    pub(crate) fn new(schema_functions: &'a HashMap<DefinitionKey, T1>, local_functions: &'a Vec<T2>) -> Self {
        Self { schema_functions, local_functions }
    }

    // pub(crate) fn empty() -> Self {
    //     Self::new(HashMap::new(), Vec::new())
    // }
}

impl<T1: GetAnnotatedSignature, T2: GetAnnotatedSignature> AnnotatedFunctionSignatures
    for AnnotatedFunctionSignaturesImpl<'_, T1, T2>
{
    fn get_annotated_signature(&self, function_id: &FunctionID) -> Option<&AnnotatedFunctionSignature> {
        match function_id {
            FunctionID::Schema(definition_key) => {
                self.schema_functions.get(definition_key).map(|getter| getter.get_annotated_signature())
            }
            &FunctionID::Preamble(index) => {
                self.local_functions.get(index).map(|getter| getter.get_annotated_signature())
            }
        }
    }
}

pub fn annotate_stored_functions(
    functions: &mut HashMap<DefinitionKey, Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<AnnotatedSchemaFunctions, Box<FunctionAnnotationError>> {
    let annotations_from_declaration = functions
        .iter()
        .map(|(id, function)| Ok((id.clone(), annotate_signature_based_on_labels(snapshot, type_manager, function)?)))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let empty_preamble_annotations = Vec::<AnnotatedFunctionSignature>::new();
    let declared_annotations =
        AnnotatedFunctionSignaturesImpl::new(&annotations_from_declaration, &empty_preamble_annotations);

    let preliminary_signature_annotations = functions
        .iter_mut()
        .map(|(function_id, function)| {
            Ok((function_id.clone(), annotate_named_function(function, snapshot, type_manager, &declared_annotations)?))
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let preliminary_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&preliminary_signature_annotations, &empty_preamble_annotations);
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotated_functions = functions
        .iter_mut()
        .map(|(id, function)| {
            Ok((
                id.clone(),
                annotate_named_function(function, snapshot, type_manager, &preliminary_signature_annotations)?,
            ))
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(annotated_functions)
}

pub fn annotate_preamble_functions(
    mut functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_signatures: Arc<AnnotatedSchemaFunctions>,
) -> Result<AnnotatedPreambleFunctions, Box<FunctionAnnotationError>> {
    let preamble_annotations_from_labels_as_map = functions
        .iter()
        .map(|function| annotate_signature_based_on_labels(snapshot, type_manager, function))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let label_based_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&schema_function_signatures, &preamble_annotations_from_labels_as_map);
    let preliminary_signature_annotations_as_map = functions
        .iter_mut()
        .map(|function| annotate_named_function(function, snapshot, type_manager, &label_based_signature_annotations))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let preliminary_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&schema_function_signatures, &preliminary_signature_annotations_as_map);
    let annotated_functions = functions
        .iter_mut()
        .map(|function| annotate_named_function(function, snapshot, type_manager, &preliminary_signature_annotations))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(annotated_functions)
}

pub(crate) fn annotate_anonymous_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    caller_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    caller_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
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
            unreachable!("The type annotations for the argument in the function call should be known by now")
        }
    }
    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        annotated_function_signatures,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

pub(super) fn annotate_named_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_some());
    let mut argument_concept_variable_types = BTreeMap::new();
    let mut argument_value_variable_types = BTreeMap::new();
    for (arg_index, (var, label)) in zip(arguments, argument_labels.as_ref().unwrap()).enumerate() {
        let argument_annotations = get_annotations_from_labels(snapshot, type_manager, label).map_err(|source| {
            Box::new(FunctionAnnotationError::CouldNotResolveArgumentType { index: arg_index, source })
        })?;
        match argument_annotations {
            FunctionParameterAnnotation::Concept(concept_annotation) => {
                argument_concept_variable_types.insert(*var, Arc::new(concept_annotation));
            }
            FunctionParameterAnnotation::Value(value_annotation) => {
                argument_value_variable_types.insert(*var, ExpressionValueType::Single(value_annotation));
            }
        }
    }
    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        annotated_function_signatures,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

fn annotate_function_impl(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    argument_concept_variable_types: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    argument_value_variable_types: BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function {
        name, context, parameters, function_body: FunctionBody { stages, return_operation }, arguments, ..
    } = function;

    let (stages, running_variable_types, running_value_types) = annotate_pipeline_stages(
        snapshot,
        type_manager,
        annotated_function_signatures,
        &mut context.variable_registry,
        parameters,
        stages.clone(),
        argument_concept_variable_types,
        argument_value_variable_types.clone(),
    )
    .map_err(|err| {
        Box::new(FunctionAnnotationError::TypeInference { name: name.to_string(), typedb_source: Box::new(err) })
    })?;
    let mapped_return = resolve_return_operators(
        snapshot,
        type_manager,
        &context.variable_registry,
        return_operation,
        &running_variable_types,
        &running_value_types,
    )?;
    let return_annotations = annotate_return(&mapped_return, &running_variable_types, &running_value_types);
    if let Some(output) = function.output.as_ref() {
        validate_return_against_signature(snapshot, type_manager, function.name.as_str(), &return_annotations, output)?;
    }
    let argument_annotations = annotate_arguments(stages.as_slice(), arguments, &argument_value_variable_types);
    let annotated_signature =
        AnnotatedFunctionSignature { arguments: argument_annotations, returned: return_annotations };
    Ok(AnnotatedFunction {
        variable_registry: context.variable_registry.clone(),
        parameter_registry: parameters.clone(),
        arguments: arguments.clone(),
        stages,
        return_: mapped_return,
        annotated_signature,
    })
}

fn validate_return_against_signature(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
    inferred_return: &[FunctionParameterAnnotation],
    signature_return: &Output,
) -> Result<(), Box<FunctionAnnotationError>> {
    let return_labels = match signature_return {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    };
    let declared_return = return_labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            get_annotations_from_labels(snapshot, type_manager, label)
                .map_err(|source| Box::new(FunctionAnnotationError::CouldNotResolveReturnType { index, source }))
        })
        .collect::<Result<Vec<_>, Box<FunctionAnnotationError>>>()?;

    debug_assert!(inferred_return.len() == declared_return.len());
    zip(inferred_return, declared_return).enumerate().try_for_each(|(i, (inferred, declared))| {
        let matches = match (&inferred, &declared) {
            (
                FunctionParameterAnnotation::Concept(inferred_types),
                FunctionParameterAnnotation::Concept(declared_types),
            ) => inferred_types.iter().all(|type_| declared_types.contains(type_)),
            (
                FunctionParameterAnnotation::Value(inferred_value),
                FunctionParameterAnnotation::Value(declared_value),
            ) => declared_value == inferred_value,
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(Box::new(FunctionAnnotationError::SignatureReturnMismatch {
                function_name: name.to_owned(),
                mismatching_index: i,
            }))
        }
    })
}

fn annotate_signature_based_on_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    function: &Function,
) -> Result<AnnotatedFunctionSignature, Box<FunctionAnnotationError>> {
    let argument_annotations: Vec<FunctionParameterAnnotation> = function
        .argument_labels
        .as_ref()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, label)| {
            get_annotations_from_labels(snapshot, type_manager, label)
                .map_err(|source| Box::new(FunctionAnnotationError::CouldNotResolveArgumentType { index, source }))
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let returned = match function.output.as_ref().unwrap() {
        Output::Stream(stream) => stream.types.iter(),
        Output::Single(single) => single.types.iter(),
    }
    .enumerate()
    .map(|(index, label)| {
        get_annotations_from_labels(snapshot, type_manager, label)
            .map_err(|source| Box::new(FunctionAnnotationError::CouldNotResolveReturnType { index, source }))
    })
    .collect::<Result<_, Box<FunctionAnnotationError>>>()?;

    Ok(AnnotatedFunctionSignature { arguments: argument_annotations, returned })
}

fn resolve_return_operators(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
    return_operation: &ReturnOperation,
    input_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunctionReturn, Box<FunctionAnnotationError>> {
    let return_ = match return_operation {
        ReturnOperation::Stream(variables) => AnnotatedFunctionReturn::Stream { variables: variables.clone() },
        ReturnOperation::Single(selector, variables) => {
            AnnotatedFunctionReturn::Single { selector: selector.clone(), variables: variables.clone() }
        }
        ReturnOperation::ReduceCheck() => AnnotatedFunctionReturn::ReduceCheck {},
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
                .map_err(|err| Box::new(FunctionAnnotationError::ReturnReduce { typedb_source: Box::new(err) }))?;
                instructions.push(instruction);
            }
            AnnotatedFunctionReturn::ReduceReducer { instructions }
        }
    };
    Ok(return_)
}

fn annotate_arguments(
    annotated_stages: &[AnnotatedStage],
    arguments: &[Variable],
    argument_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Vec<FunctionParameterAnnotation> {
    let first_match_annotations = annotated_stages
        .iter()
        .filter_map(|stage| {
            if let AnnotatedStage::Match { block_annotations, .. } = stage {
                Some(block_annotations)
            } else {
                None
            }
        })
        .next()
        .unwrap();
    arguments
        .iter()
        .map(|&var| {
            get_function_parameter(var, first_match_annotations.vertex_annotations(), argument_value_type_annotations)
        })
        .collect()
}

fn annotate_return(
    return_operation: &AnnotatedFunctionReturn,
    final_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    final_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Vec<FunctionParameterAnnotation> {
    match return_operation {
        AnnotatedFunctionReturn::Stream { variables } | AnnotatedFunctionReturn::Single { variables, .. } => variables
            .iter()
            .map(|&var| get_function_parameter(var, final_type_annotations, final_value_type_annotations))
            .collect(),
        AnnotatedFunctionReturn::ReduceReducer { instructions } => instructions
            .iter()
            .map(|instruction| FunctionParameterAnnotation::Value(instruction.output_type()))
            .collect(),
        AnnotatedFunctionReturn::ReduceCheck {} => vec![FunctionParameterAnnotation::Value(ValueType::Boolean)],
    }
}

fn get_function_parameter<V: From<Variable> + Ord>(
    variable: Variable,
    body_variable_annotations: &BTreeMap<V, Arc<BTreeSet<Type>>>,
    body_variable_value_types: &BTreeMap<Variable, ExpressionValueType>,
) -> FunctionParameterAnnotation {
    if let Some(arced_types) = body_variable_annotations.get(&variable.into()) {
        let types: &BTreeSet<Type> = arced_types;
        FunctionParameterAnnotation::Concept(types.clone())
    } else if let Some(expression_value_type) = body_variable_value_types.get(&variable) {
        FunctionParameterAnnotation::Value(expression_value_type.value_type().clone())
    } else {
        unreachable!("Could not find annotations for a function return variable.")
    }
}

fn get_annotations_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    typeql_label: &TypeRefAny,
) -> Result<FunctionParameterAnnotation, TypeInferenceError> {
    let TypeRef::Named(inner_type) = (match typeql_label {
        TypeRefAny::Type(inner) => inner,
        TypeRefAny::Optional(typeql::type_::Optional { inner: _inner, .. }) => todo!(),
        TypeRefAny::List(typeql::type_::List { inner: _inner, .. }) => todo!(),
    }) else {
        unreachable!("Function return labels cannot be variable.");
    };
    match inner_type {
        NamedType::Label(label) => {
            // TODO: could be a struct value type in the future!
            let types = type_seeder::get_type_annotation_and_subtypes_from_label(
                snapshot,
                type_manager,
                &Label::build(label.ident.as_str_unchecked()),
            )?;
            Ok(FunctionParameterAnnotation::Concept(types))
        }
        NamedType::BuiltinValueType(value_type) => {
            // TODO: This may be list
            let value = translate_value_type(&value_type.token);
            Ok(FunctionParameterAnnotation::Value(value))
        }
        NamedType::Role(_) => unreachable!("A function return label was wrongly parsed as role-type."),
    }
}

pub struct EmptyAnnotatedFunctionSignatures;
impl AnnotatedFunctionSignatures for EmptyAnnotatedFunctionSignatures {
    fn get_annotated_signature(&self, _function_id: &FunctionID) -> Option<&AnnotatedFunctionSignature> {
        None
    }
}
