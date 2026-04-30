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
use error::needs_update_when_feature_is_implemented;
use ir::{
    pattern::{expression::BuiltinConceptFunctionID, Vertex},
    pipeline::{
        function::{Function, FunctionBody, ReturnOperation}, function_signature::FunctionID,
        ParameterRegistry,
        VariableRegistry,
    },
    translation::tokens::translate_value_type,
};
use storage::snapshot::ReadableSnapshot;
use typeql::{
    common::{Span, Spanned},
    schema::definable::function::{Output, SingleSelector},
    type_::{NamedType, NamedTypeAny},
};

use crate::{
    annotation::{
        expression::compiled_expression::ExpressionValueType, pipeline::{
            annotate_pipeline_stages, resolve_reducer_by_value_type, AnnotatedStage, RunningVariableAnnotations,
        },
        type_seeder,
        FunctionAnnotationError,
        TypeInferenceError,
    },
    executable::reduce::ReduceInstruction,
};
use crate::annotation::utils::{AnnotationContext, PipelineAnnotationContext};

#[derive(Debug, Clone)]
pub enum FunctionParameterAnnotation {
    AnyConcept,
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
    pub is_stream: bool,
    pub arguments: Vec<FunctionParameterAnnotation>,
    pub returns: Vec<FunctionParameterAnnotation>,
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
    fn get_annotated_signature(&self, function_id: &FunctionID) -> Option<Cow<'_, AnnotatedFunctionSignature>>;
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
}

impl<T1: GetAnnotatedSignature, T2: GetAnnotatedSignature> AnnotatedFunctionSignatures
    for AnnotatedFunctionSignaturesImpl<'_, T1, T2>
{
    fn get_annotated_signature(&self, function_id: &FunctionID) -> Option<Cow<'_, AnnotatedFunctionSignature>> {
        match function_id {
            &FunctionID::Builtin(builtin_id) => Some(Cow::Owned(get_builtin_function_annotated_signature(builtin_id))),
            FunctionID::Schema(definition_key) => {
                self.schema_functions.get(definition_key).map(|getter| Cow::Borrowed(getter.get_annotated_signature()))
            }
            &FunctionID::Preamble(index) => {
                self.local_functions.get(index).map(|getter| Cow::Borrowed(getter.get_annotated_signature()))
            }
        }
    }
}

fn get_builtin_function_annotated_signature(builtin_id: BuiltinConceptFunctionID) -> AnnotatedFunctionSignature {
    match builtin_id {
        BuiltinConceptFunctionID::Iid => AnnotatedFunctionSignature {
            is_stream: false,
            arguments: vec![FunctionParameterAnnotation::AnyConcept],
            returns: vec![FunctionParameterAnnotation::Value(ValueType::String)],
        },
        BuiltinConceptFunctionID::Label => AnnotatedFunctionSignature {
            is_stream: false,
            arguments: vec![FunctionParameterAnnotation::AnyConcept],
            returns: vec![FunctionParameterAnnotation::Value(ValueType::String)],
        },
    }
}

pub fn annotate_stored_functions(
    functions: &mut HashMap<DefinitionKey, Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<AnnotatedSchemaFunctions, Box<FunctionAnnotationError>> {
    let label_ctx = AnnotationContext::new(snapshot, type_manager, &EmptyAnnotatedFunctionSignatures);
    let annotations_from_declaration = functions
        .iter()
        .map(|(id, function)| Ok((id.clone(), annotate_signature_based_on_labels(&label_ctx, function)?)))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let empty_preamble_annotations = Vec::<AnnotatedFunctionSignature>::new();
    let declared_annotations =
        AnnotatedFunctionSignaturesImpl::new(&annotations_from_declaration, &empty_preamble_annotations);

    let preliminary_signature_annotations = functions
        .iter_mut()
        .map(|(function_id, function)| {
            let ctx = AnnotationContext::new(snapshot, type_manager, &declared_annotations);
            Ok((function_id.clone(), annotate_named_function(function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures)?))
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let preliminary_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&preliminary_signature_annotations, &empty_preamble_annotations);
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotated_functions = functions
        .iter_mut()
        .map(|(id, function)| {
            let ctx = AnnotationContext::new(snapshot, type_manager, &preliminary_signature_annotations);
            Ok((id.clone(), annotate_named_function(function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures)?))
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
    let label_ctx = AnnotationContext::new(snapshot, type_manager, &EmptyAnnotatedFunctionSignatures);
    let preamble_annotations_from_labels_as_map = functions
        .iter()
        .map(|function| annotate_signature_based_on_labels(&label_ctx, function))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let label_based_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&schema_function_signatures, &preamble_annotations_from_labels_as_map);
    let preliminary_signature_annotations_as_map = functions
        .iter_mut()
        .map(|function| {
            let ctx = AnnotationContext::new(snapshot, type_manager, &label_based_signature_annotations);
            annotate_named_function(function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures)
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let preliminary_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&schema_function_signatures, &preliminary_signature_annotations_as_map);
    let annotated_functions = functions
        .iter_mut()
        .map(|function| {
            let ctx = AnnotationContext::new(snapshot, type_manager, &preliminary_signature_annotations);
            annotate_named_function(function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures)
        })
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
    input_annotations: &RunningVariableAnnotations,
    _source_span: Option<Span>,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_none());
    let ctx = AnnotationContext::new(snapshot, type_manager, annotated_function_signatures);
    let argument_types_iter = arguments.iter().map(|var| {
        let arg_type = input_annotations
            .get_param(var)
            .expect("The type annotations for the argument in the function call should be known by now");
        (*var, arg_type)
    });
    let argument_annotations = RunningVariableAnnotations::from_iterator(argument_types_iter);
    annotate_function_impl(&ctx, function, argument_annotations)
}

pub(super) fn annotate_named_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function { arguments, argument_labels, .. } = function;
    let ctx = AnnotationContext::new(snapshot, type_manager, annotated_function_signatures);
    debug_assert!(argument_labels.is_some());
    let arg_labels = argument_labels.as_ref().unwrap();
    let types = get_annotations_from_labels_vec(&ctx, arg_labels).map_err(
        |(index, source_span, typedb_source)| {
            Box::new(FunctionAnnotationError::CouldNotResolveArgumentType { index, source_span, typedb_source })
        },
    )?;
    let argument_annotations =
        RunningVariableAnnotations::from_iterator(zip(arguments.iter().copied(), types.into_iter()));
    annotate_function_impl(&ctx, function, argument_annotations)
}

fn annotate_function_impl(
    annotation_ctx: &AnnotationContext<'_, impl ReadableSnapshot>,
    function: &mut Function,
    argument_annotations_from_declaration: RunningVariableAnnotations,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function {
        name, context, parameters, function_body: FunctionBody { stages, return_operation }, arguments, ..
    } = function;
    let ctx = PipelineAnnotationContext::new(annotation_ctx.snapshot, annotation_ctx.type_manager, annotation_ctx.annotated_function_signatures, &mut context.variable_registry, parameters);
    let (stages, output_annotations) = annotate_pipeline_stages(
        &ctx,
        stages.clone(),
        argument_annotations_from_declaration.clone(),
        Some(return_operation.variables().as_ref()),
    )
    .map_err(|err| {
        Box::new(FunctionAnnotationError::TypeInference { name: name.to_string(), typedb_source: Box::new(err) })
    })?;
    let mapped_return = resolve_return_operators(&ctx, return_operation, &output_annotations)?;
    let return_annotations = annotate_return(&mapped_return, &output_annotations);
    if let Some(output) = function.output.as_ref() {
        validate_return_against_signature(&annotation_ctx, function.name.as_str(), &return_annotations, output)?;
    }
    let argument_annotations = annotate_arguments(stages.as_slice(), arguments, &argument_annotations_from_declaration);
    let is_stream = matches!(function.output, Some(Output::Stream(_)));
    let annotated_signature =
        AnnotatedFunctionSignature { is_stream, arguments: argument_annotations, returns: return_annotations };
    Ok(AnnotatedFunction {
        variable_registry: ctx.variable_registry.clone(),
        parameter_registry: parameters.clone(),
        arguments: arguments.clone(),
        stages,
        return_: mapped_return,
        annotated_signature,
    })
}

fn validate_return_against_signature(
    ctx: &AnnotationContext<'_, impl ReadableSnapshot>,
    name: &str,
    inferred_return: &[FunctionParameterAnnotation],
    signature_return: &Output,
) -> Result<(), Box<FunctionAnnotationError>> {
    let return_labels = match signature_return {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    };
    let declared_return = get_annotations_from_labels_vec(ctx, return_labels.as_slice()).map_err(
        |(index, _span, typedb_source)| {
            Box::new(FunctionAnnotationError::CouldNotResolveReturnType { index, typedb_source })
        },
    )?;

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
                source_span: signature_return.span(),
            }))
        }
    })
}

fn annotate_signature_based_on_labels(
    ctx: &AnnotationContext<'_, impl ReadableSnapshot>,
    function: &Function,
) -> Result<AnnotatedFunctionSignature, Box<FunctionAnnotationError>> {
    let argument_labels = function.argument_labels.as_ref().unwrap();
    let argument_annotations = get_annotations_from_labels_vec(ctx, argument_labels).map_err(
        |(index, source_span, typedb_source)| {
            Box::new(FunctionAnnotationError::CouldNotResolveArgumentType { index, source_span, typedb_source })
        },
    )?;
    let return_labels = match function.output.as_ref().unwrap() {
        Output::Stream(stream) => stream.types.as_slice(),
        Output::Single(single) => single.types.as_slice(),
    };
    let returned = get_annotations_from_labels_vec(ctx, &return_labels).map_err(
        |(index, _span, typedb_source)| {
            Box::new(FunctionAnnotationError::CouldNotResolveReturnType { index, typedb_source })
        },
    )?;
    let is_stream = matches!(function.output, Some(Output::Stream(_)));
    Ok(AnnotatedFunctionSignature { is_stream, arguments: argument_annotations, returns: returned })
}

fn resolve_return_operators(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    return_operation: &ReturnOperation,
    output_annotations: &RunningVariableAnnotations,
) -> Result<AnnotatedFunctionReturn, Box<FunctionAnnotationError>> {
    let return_ = match return_operation {
        ReturnOperation::Stream(variables, _) => AnnotatedFunctionReturn::Stream { variables: variables.clone() },
        ReturnOperation::Single(selector, variables, _) => {
            AnnotatedFunctionReturn::Single { selector: selector.clone(), variables: variables.clone() }
        }
        ReturnOperation::ReduceCheck(_) => AnnotatedFunctionReturn::ReduceCheck {},
        ReturnOperation::ReduceReducer(reducers, source_span) => {
            let mut instructions = Vec::with_capacity(reducers.len());
            for &reducer in reducers {
                let instruction =
                    resolve_reducer_by_value_type(ctx, reducer, output_annotations, *source_span)
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
    argument_annotations_from_signature: &RunningVariableAnnotations,
) -> Vec<FunctionParameterAnnotation> {
    arguments
        .iter()
        .map(|&var| {
            let body_variable_annotations = annotated_stages
                .iter()
                .filter_map(|stage| {
                    if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
                        let root_annotations = block_annotations.type_annotations_of(block.conjunction()).unwrap();
                        root_annotations.vertex_annotations_of(&Vertex::Variable(var)).cloned()
                    } else {
                        None
                    }
                })
                .next();
            if let Some(arced_types) = body_variable_annotations.as_ref() {
                let types: &BTreeSet<Type> = arced_types;
                FunctionParameterAnnotation::Concept(types.clone())
            } else if let Some(arced_types) = argument_annotations_from_signature.concepts.get(&var) {
                let types: &BTreeSet<Type> = arced_types;
                FunctionParameterAnnotation::Concept(types.clone())
            } else if let Some(expression_value_type) = argument_annotations_from_signature.values.get(&var) {
                FunctionParameterAnnotation::Value(expression_value_type.value_type().clone())
            } else {
                unreachable!("Could not find annotations for a function argument or return variable.")
            }
        })
        .collect()
}

fn annotate_return(
    return_operation: &AnnotatedFunctionReturn,
    final_stage_annotations: &RunningVariableAnnotations,
) -> Vec<FunctionParameterAnnotation> {
    match return_operation {
        AnnotatedFunctionReturn::Stream { variables } | AnnotatedFunctionReturn::Single { variables, .. } => variables
            .iter()
            .map(|&var| {
                if let Some(arced_types) = final_stage_annotations.concepts.get(&var) {
                    let types: &BTreeSet<Type> = arced_types;
                    FunctionParameterAnnotation::Concept(types.clone())
                } else if let Some(expression_value_type) = final_stage_annotations.values.get(&var) {
                    FunctionParameterAnnotation::Value(expression_value_type.value_type().clone())
                } else {
                    unreachable!("Could not find annotations for a function argument or return variable.")
                }
            })
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
        unreachable!("Could not find annotations for a function argument or return variable.")
    }
}

pub(super) fn get_annotations_from_labels_vec(
    ctx: &AnnotationContext<'_, impl ReadableSnapshot>,
    typeql_labels: &[NamedTypeAny],
) -> Result<Vec<FunctionParameterAnnotation>, (usize, Option<Span>, TypeInferenceError)> {
    typeql_labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            get_annotations_from_labels(ctx, label).map_err(|err| (index, typeql_labels[index].span(), err))
        })
        .collect::<Result<Vec<_>, _>>()
}

pub(super) fn get_annotations_from_labels(
    ctx: &AnnotationContext<'_, impl ReadableSnapshot>,
    typeql_label: &NamedTypeAny,
) -> Result<FunctionParameterAnnotation, TypeInferenceError> {
    let named_type = match typeql_label {
        NamedTypeAny::Simple(inner) => inner,
        NamedTypeAny::Optional(typeql::type_::NamedTypeOptional { inner, .. }) => inner,
        NamedTypeAny::List(typeql::type_::NamedTypeList { .. }) => {
            needs_update_when_feature_is_implemented!(Lists);
            return Err(TypeInferenceError::ListTypesUnsupported {});
        }
    };
    match named_type {
        NamedType::Label(label) => {
            // TODO: could be a struct value type in the future!
            let types = type_seeder::get_type_annotation_and_subtypes_from_label(
                ctx.snapshot,
                ctx.type_manager,
                &Label::build(label.ident.as_str_unchecked(), label.span()),
            )?;
            Ok(FunctionParameterAnnotation::Concept(types))
        }
        NamedType::BuiltinValueType(value_type) => {
            // TODO: This may be list
            let value = translate_value_type(&value_type.token);
            Ok(FunctionParameterAnnotation::Value(value))
        }
    }
}

pub struct EmptyAnnotatedFunctionSignatures;
impl AnnotatedFunctionSignatures for EmptyAnnotatedFunctionSignatures {
    fn get_annotated_signature(&self, _function_id: &FunctionID) -> Option<Cow<'_, AnnotatedFunctionSignature>> {
        None
    }
}
