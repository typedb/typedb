/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    iter,
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::{ValueType, ValueTypeCategory};
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{
        block::Block,
        fetch::FetchObject,
        function::Function,
        modifier::{Limit, Offset, Require, Select, Sort},
        reduce::{AssignedReduction, Reduce, Reducer},
        ParameterRegistry, VariableRegistry,
    },
    translation::pipeline::TranslatedStage,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::{
        expression::{
            block_compiler::compile_expressions,
            compiled_expression::{ExecutableExpression, ExpressionValueType},
            ExpressionCompileError,
        },
        fetch::{annotate_fetch, AnnotatedFetch},
        function::{
            annotate_preamble_functions, AnnotatedFunctionSignatures, AnnotatedFunctionSignaturesImpl,
            AnnotatedPreambleFunctions, AnnotatedSchemaFunctions, FunctionParameterAnnotation,
        },
        match_inference::infer_types,
        type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::resolve_value_types,
        AnnotationError,
    },
    executable::{insert::type_check::check_annotations, reduce::ReduceInstruction},
};

pub struct AnnotatedPipeline {
    pub annotated_preamble: AnnotatedPreambleFunctions,
    pub annotated_stages: Vec<AnnotatedStage>,
    pub annotated_fetch: Option<AnnotatedFetch>,
}

#[derive(Debug, Clone)]
pub enum AnnotatedStage {
    Match {
        block: Block,
        block_annotations: TypeAnnotations,
        // expressions skip annotation and go straight to executable, breaking the abstraction a bit...
        executable_expressions: HashMap<Variable, ExecutableExpression<Variable>>,
    },
    Insert {
        block: Block,
        annotations: TypeAnnotations,
    },
    Delete {
        block: Block,
        deleted_variables: Vec<Variable>,
        annotations: TypeAnnotations,
    },
    // ...
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Require(Require),
    Reduce(Reduce, Vec<ReduceInstruction<Variable>>),
}

impl AnnotatedStage {
    pub fn named_referenced_variables<'a>(
        &'a self,
        variable_registry: &'a VariableRegistry,
    ) -> impl Iterator<Item = Variable> + '_ {
        let variables: Box<dyn Iterator<Item = Variable> + '_> = match self {
            AnnotatedStage::Match { block, .. } => Box::new(block.variables()),
            AnnotatedStage::Insert { block, .. } => Box::new(block.variables()),
            AnnotatedStage::Delete { block, .. } => Box::new(block.variables()),
            AnnotatedStage::Select(select) => Box::new(select.variables.iter().cloned()),
            AnnotatedStage::Sort(sort) => Box::new(sort.variables.iter().map(|sort_variable| sort_variable.variable())),
            AnnotatedStage::Offset(_) => Box::new(iter::empty()),
            AnnotatedStage::Limit(_) => Box::new(iter::empty()),
            AnnotatedStage::Require(_) => Box::new(iter::empty()),
            AnnotatedStage::Reduce(reduce, _) => Box::new(reduce.variables()),
        };
        variables.filter(move |variable| variable_registry.get_variable_name(*variable).is_some())
    }
}

pub fn annotate_preamble_and_pipeline(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: Arc<AnnotatedSchemaFunctions>,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    translated_preamble: Vec<Function>,
    translated_stages: Vec<TranslatedStage>,
    translated_fetch: Option<FetchObject>,
) -> Result<AnnotatedPipeline, AnnotationError> {
    let annotated_preamble =
        annotate_preamble_functions(translated_preamble, snapshot, type_manager, schema_function_annotations.clone())
            .map_err(|typedb_source| AnnotationError::PreambleTypeInference { typedb_source })?;
    let combined_signature_annotations =
        AnnotatedFunctionSignaturesImpl::new(&schema_function_annotations, &annotated_preamble);
    let (annotated_stages, annotated_fetch) = annotate_stages_and_fetch(
        snapshot,
        type_manager,
        &combined_signature_annotations,
        variable_registry,
        parameters,
        translated_stages,
        translated_fetch,
        BTreeMap::new(),
        BTreeMap::new(),
    )?;
    Ok(AnnotatedPipeline { annotated_stages, annotated_fetch, annotated_preamble })
}

pub(crate) fn annotate_stages_and_fetch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    translated_stages: Vec<TranslatedStage>,
    translated_fetch: Option<FetchObject>,
    input_type_annotations: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_type_annotations: BTreeMap<Variable, ExpressionValueType>,
) -> Result<(Vec<AnnotatedStage>, Option<AnnotatedFetch>), AnnotationError> {
    let (annotated_stages, running_variable_annotations, running_value_variable_types) = annotate_pipeline_stages(
        snapshot,
        type_manager,
        annotated_function_signatures,
        variable_registry,
        parameters,
        translated_stages,
        input_type_annotations,
        input_value_type_annotations,
    )?;
    let annotated_fetch = match translated_fetch {
        None => None,
        Some(fetch) => {
            let annotated = annotate_fetch(
                fetch,
                snapshot,
                type_manager,
                variable_registry,
                parameters,
                annotated_function_signatures,
                &running_variable_annotations,
                &running_value_variable_types,
            );
            Some(annotated?)
        }
    };
    Ok((annotated_stages, annotated_fetch))
}

pub(crate) fn annotate_pipeline_stages(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    translated_stages: Vec<TranslatedStage>,
    input_type_annotations: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_type_annotations: BTreeMap<Variable, ExpressionValueType>,
) -> Result<
    (Vec<AnnotatedStage>, BTreeMap<Variable, Arc<BTreeSet<Type>>>, BTreeMap<Variable, ExpressionValueType>),
    AnnotationError,
> {
    let mut running_variable_annotations: BTreeMap<Variable, Arc<BTreeSet<Type>>> = input_type_annotations;
    let mut running_value_variable_types: BTreeMap<Variable, ExpressionValueType> = input_value_type_annotations;
    let mut annotated_stages = Vec::with_capacity(translated_stages.len());

    let empty_constraint_annotations = HashMap::new();
    let mut latest_match_index = None;
    for stage in translated_stages {
        let running_constraint_annotations = latest_match_index
            .map(|idx| {
                let AnnotatedStage::Match { block_annotations, .. } = annotated_stages.get(idx).unwrap() else {
                    unreachable!("LatestMatchIndex will always be a match");
                };
                block_annotations.constraint_annotations()
            })
            .unwrap_or(&empty_constraint_annotations);
        let annotated_stage = annotate_stage(
            &mut running_variable_annotations,
            &mut running_value_variable_types,
            variable_registry,
            parameters,
            snapshot,
            type_manager,
            annotated_function_signatures,
            running_constraint_annotations,
            stage,
        )?;
        if let AnnotatedStage::Match { .. } = annotated_stage {
            latest_match_index = Some(annotated_stages.len());
        }
        annotated_stages.push(annotated_stage);
    }
    Ok((annotated_stages, running_variable_annotations, running_value_variable_types))
}

fn annotate_stage(
    running_variable_annotations: &mut BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    running_value_variable_assigned_types: &mut BTreeMap<Variable, ExpressionValueType>,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    running_constraint_annotations: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    stage: TranslatedStage,
) -> Result<AnnotatedStage, AnnotationError> {
    match stage {
        TranslatedStage::Match { block } => {
            let block_annotations = infer_types(
                snapshot,
                &block,
                variable_registry,
                type_manager,
                running_variable_annotations,
                annotated_function_signatures,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            block_annotations.vertex_annotations().iter().for_each(|(vertex, types)| {
                if let Some(var) = vertex.as_variable() {
                    running_variable_annotations.insert(var, types.clone());
                }
            });

            collect_value_types_of_function_call_assignments(
                block.conjunction(),
                annotated_function_signatures,
                running_value_variable_assigned_types,
                variable_registry,
            )?;

            let compiled_expressions = compile_expressions(
                snapshot,
                type_manager,
                &block,
                variable_registry,
                parameters,
                &block_annotations,
                running_value_variable_assigned_types,
            )
            .map_err(|source| AnnotationError::ExpressionCompilation { source })?;
            compiled_expressions.iter().for_each(|(&variable, expr)| {
                running_value_variable_assigned_types.insert(variable, expr.return_type().clone());
            });
            Ok(AnnotatedStage::Match { block, block_annotations, executable_expressions: compiled_expressions })
        }

        TranslatedStage::Insert { block } => {
            let insert_annotations = infer_types(
                snapshot,
                &block,
                variable_registry,
                type_manager,
                running_variable_annotations,
                annotated_function_signatures,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            block.conjunction().constraints().iter().for_each(|constraint| match constraint {
                Constraint::Isa(isa) => {
                    running_variable_annotations.insert(
                        isa.thing().as_variable().unwrap(),
                        insert_annotations.vertex_annotations_of(isa.thing()).unwrap().clone(),
                    );
                }
                Constraint::RoleName(role_name) => {
                    running_variable_annotations.insert(
                        role_name.type_().as_variable().unwrap(),
                        insert_annotations.vertex_annotations_of(role_name.type_()).unwrap().clone(),
                    );
                }
                Constraint::Links(links) => {
                    if let Some(variable) = links.role_type().as_variable() {
                        if !running_variable_annotations.contains_key(&variable)
                            && insert_annotations.vertex_annotations_of(links.role_type()).is_some()
                        {
                            running_variable_annotations.insert(
                                variable,
                                insert_annotations.vertex_annotations_of(links.role_type()).unwrap().clone(),
                            );
                        }
                    }
                }
                _ => (),
            });
            check_annotations(
                snapshot,
                type_manager,
                &block,
                running_variable_annotations,
                running_constraint_annotations,
                &insert_annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            Ok(AnnotatedStage::Insert { block, annotations: insert_annotations })
        }

        TranslatedStage::Delete { block, deleted_variables } => {
            let delete_annotations = infer_types(
                snapshot,
                &block,
                variable_registry,
                type_manager,
                running_variable_annotations,
                annotated_function_signatures,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            deleted_variables.iter().for_each(|v| {
                running_variable_annotations.remove(v);
            });
            // TODO: check_annotations on deletes. Can only delete links or has for types that actually are linked or owned
            Ok(AnnotatedStage::Delete { block, deleted_variables, annotations: delete_annotations })
        }
        TranslatedStage::Sort(sort) => {
            validate_sort_variables_comparable(
                &sort,
                running_variable_annotations,
                running_value_variable_assigned_types,
                snapshot,
                type_manager,
                variable_registry,
            )?;
            Ok(AnnotatedStage::Sort(sort))
        }
        TranslatedStage::Select(select) => Ok(AnnotatedStage::Select(select)),
        TranslatedStage::Offset(offset) => Ok(AnnotatedStage::Offset(offset)),
        TranslatedStage::Limit(limit) => Ok(AnnotatedStage::Limit(limit)),
        TranslatedStage::Require(require) => Ok(AnnotatedStage::Require(require)),

        TranslatedStage::Reduce(reduce) => {
            let mut reduce_instructions = Vec::with_capacity(reduce.assigned_reductions.len());
            for &AssignedReduction { assigned, reduction } in &reduce.assigned_reductions {
                let typed_reduce = resolve_reducer_by_value_type(
                    snapshot,
                    type_manager,
                    variable_registry,
                    reduction,
                    running_variable_annotations,
                    running_value_variable_assigned_types,
                )?;
                running_value_variable_assigned_types
                    .insert(assigned, ExpressionValueType::Single(typed_reduce.output_type().clone()));
                reduce_instructions.push(typed_reduce);
            }
            Ok(AnnotatedStage::Reduce(reduce, reduce_instructions))
        }
    }
}

pub fn validate_sort_variables_comparable(
    sort: &Sort,
    variable_annotations: &mut BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    assigned_value_types: &mut BTreeMap<Variable, ExpressionValueType>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
) -> Result<(), AnnotationError> {
    for sort_var in &sort.variables {
        if assigned_value_types.contains_key(&sort_var.variable()) {
            continue;
        } else if let Some(types) = variable_annotations.get(&sort_var.variable()) {
            let value_types = resolve_value_types(types, snapshot, type_manager)
                .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            if value_types.is_empty() {
                let variable_name = variable_registry.variable_names().get(&sort_var.variable()).unwrap().clone();
                return Err(AnnotationError::CouldNotDetermineValueTypeForReducerInput { variable: variable_name });
            }
            let first_category = value_types.iter().find(|_| true).unwrap().category();
            let allowed_categories = ValueTypeCategory::comparable_categories(first_category);
            for other_type in value_types.iter().map(|v| v.category()) {
                // Don't need to do pairwise if comparable is transitive
                if !allowed_categories.contains(&other_type) {
                    let variable_name = variable_registry.variable_names().get(&sort_var.variable()).unwrap().clone();
                    return Err(AnnotationError::UncomparableValueTypesForSortVariable {
                        variable: variable_name,
                        category1: first_category,
                        category2: other_type,
                    });
                }
            }
        } else {
            unreachable!()
        }
    }
    Ok(())
}

pub fn resolve_reducer_by_value_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
    reducer: Reducer,
    variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    assigned_value_types: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<ReduceInstruction<Variable>, AnnotationError> {
    match reducer {
        Reducer::Count => Ok(ReduceInstruction::Count),
        Reducer::CountVar(variable) => Ok(ReduceInstruction::CountVar(variable)),
        Reducer::Sum(variable)
        | Reducer::Max(variable)
        | Reducer::Mean(variable)
        | Reducer::Median(variable)
        | Reducer::Min(variable)
        | Reducer::Std(variable) => {
            let value_type = determine_value_type_for_reducer(
                reducer,
                variable,
                variable_annotations,
                assigned_value_types,
                snapshot,
                type_manager,
                variable_registry,
            )?;
            resolve_reduce_instruction_by_value_type(reducer, value_type, variable_registry)
        }
    }
}

fn determine_value_type_for_reducer(
    reducer: Reducer,
    variable: Variable,
    variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    assigned_value_types: &BTreeMap<Variable, ExpressionValueType>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
) -> Result<ValueType, AnnotationError> {
    if let Some(assigned_type) = assigned_value_types.get(&variable) {
        match assigned_type {
            ExpressionValueType::Single(value_type) => Ok(value_type.clone()),
            ExpressionValueType::List(_) => {
                let variable_name = variable_registry.variable_names()[&variable].clone();
                Err(AnnotationError::ReducerInputVariableIsList { reducer: reducer.name(), variable: variable_name })
            }
        }
    } else if let Some(types) = variable_annotations.get(&variable) {
        let value_types = resolve_value_types(types, snapshot, type_manager)
            .map_err(|source| AnnotationError::TypeInference { typedb_source: source })?;
        if value_types.len() != 1 {
            let variable_name = variable_registry.variable_names()[&variable].clone();
            Err(AnnotationError::ReducerInputVariableDidNotHaveSingleValueType { variable: variable_name })
        } else {
            Ok(value_types.iter().next().unwrap().clone())
        }
    } else {
        let variable_name = variable_registry.variable_names()[&variable].clone();
        Err(AnnotationError::CouldNotDetermineValueTypeForReducerInput { variable: variable_name })
    }
}

pub fn resolve_reduce_instruction_by_value_type(
    reducer: Reducer,
    value_type: ValueType,
    variable_registry: &VariableRegistry,
) -> Result<ReduceInstruction<Variable>, AnnotationError> {
    // Will have been handled earlier since it doesn't need a value type.
    debug_assert!(!matches!(reducer, Reducer::Count) && !matches!(reducer, Reducer::CountVar(_)));
    match value_type.category() {
        ValueTypeCategory::Integer => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Sum(var) => Ok(ReduceInstruction::SumInteger(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxInteger(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinInteger(var)),
            Reducer::Mean(var) => Ok(ReduceInstruction::MeanInteger(var)),
            Reducer::Median(var) => Ok(ReduceInstruction::MedianInteger(var)),
            Reducer::Std(var) => Ok(ReduceInstruction::StdInteger(var)),
        },
        ValueTypeCategory::Double => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Sum(var) => Ok(ReduceInstruction::SumDouble(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxDouble(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinDouble(var)),
            Reducer::Mean(var) => Ok(ReduceInstruction::MeanDouble(var)),
            Reducer::Median(var) => Ok(ReduceInstruction::MedianDouble(var)),
            Reducer::Std(var) => Ok(ReduceInstruction::StdDouble(var)),
        },
        _ => {
            let var = match reducer {
                Reducer::Count => unreachable!(),
                Reducer::CountVar(var)
                | Reducer::Sum(var)
                | Reducer::Max(var)
                | Reducer::Mean(var)
                | Reducer::Median(var)
                | Reducer::Min(var)
                | Reducer::Std(var) => var,
            };
            let reducer_name = reducer.name();
            let variable_name = variable_registry.variable_names()[&var].clone();
            Err(AnnotationError::UnsupportedValueTypeForReducer {
                reducer: reducer_name,
                variable: variable_name,
                value_type: value_type.category(),
            })
        }
    }
}

fn collect_value_types_of_function_call_assignments(
    conjunction: &Conjunction,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    value_type_annotations: &mut BTreeMap<Variable, ExpressionValueType>,
    variable_registry: &VariableRegistry,
) -> Result<(), AnnotationError> {
    conjunction
        .constraints()
        .iter()
        .filter_map(|constraint| match constraint {
            Constraint::FunctionCallBinding(binding) => Some(binding),
            _ => None,
        })
        .try_for_each(|binding| {
            let return_ = &annotated_function_signatures
                .get_annotated_signature(&binding.function_call().function_id())
                .unwrap()
                .returned;
            zip(binding.assigned(), return_.iter()).try_for_each(|(var, annotation)| match &annotation {
                FunctionParameterAnnotation::Value(value_type) => {
                    if value_type_annotations.contains_key(&var.as_variable().unwrap()) {
                        let assign_variable = variable_registry.get_variable_name(var.as_variable().unwrap()).cloned();
                        return Err(AnnotationError::ExpressionCompilation {
                            source: Box::new(ExpressionCompileError::MultipleAssignmentsForSingleVariable {
                                assign_variable,
                            }),
                        });
                    }
                    value_type_annotations
                        .insert(var.as_variable().unwrap(), ExpressionValueType::Single(value_type.clone()));
                    Ok(())
                }
                FunctionParameterAnnotation::Concept(_) => Ok(()),
            })
        })?;
    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPattern::Disjunction(disjunction) => disjunction.conjunctions().iter().try_for_each(|inner| {
            collect_value_types_of_function_call_assignments(
                inner,
                annotated_function_signatures,
                value_type_annotations,
                variable_registry,
            )
        }),
        NestedPattern::Negation(negation) => collect_value_types_of_function_call_assignments(
            negation.conjunction(),
            annotated_function_signatures,
            value_type_annotations,
            variable_registry,
        ),
        NestedPattern::Optional(optional) => collect_value_types_of_function_call_assignments(
            optional.conjunction(),
            annotated_function_signatures,
            value_type_annotations,
            variable_registry,
        ),
    })?;
    Ok(())
}
