/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    iter::zip,
    sync::Arc,
};

use answer::{Type, variable::Variable};
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::{ValueType, ValueTypeCategory};
use error::needs_update_when_feature_is_implemented;
use ir::{
    pattern::{
        Vertex,
        conjunction::Conjunction,
        constraint::{Constraint, ExpressionBinding},
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
    },
    pipeline::{
        ParameterRegistry, VariableRegistry,
        block::Block,
        fetch::FetchObject,
        function::Function,
        modifier::{Distinct, Limit, Offset, Require, Select, Sort},
        reduce::{AssignedReduction, Reduce, Reducer},
    },
    translation::pipeline::TranslatedStage,
};
use storage::snapshot::ReadableSnapshot;
use typeql::common::Span;

use crate::{
    annotation::{
        AnnotationError,
        expression::{
            ExpressionCompileError,
            block_compiler::compile_expressions,
            compiled_expression::{ExecutableExpression, ExpressionValueType},
        },
        fetch::{AnnotatedFetch, annotate_fetch},
        function::{
            AnnotatedFunctionSignatures, AnnotatedFunctionSignaturesImpl, AnnotatedPreambleFunctions,
            AnnotatedSchemaFunctions, FunctionParameterAnnotation, annotate_preamble_functions,
        },
        match_inference::infer_types,
        type_annotations::{BlockAnnotations, ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::resolve_value_types,
        utils::PipelineAnnotationContext,
        write_type_check::check_type_combinations_for_write,
    },
    executable::{reduce::ReduceInstruction, update},
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
        block_annotations: BlockAnnotations,
        // expressions skip annotation and go straight to executable, breaking the abstraction a bit...
        executable_expressions: HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
        source_span: Option<Span>,
    },
    Insert {
        block: Block,
        annotations: BlockAnnotations,
        source_span: Option<Span>,
    },
    Update {
        block: Block,
        annotations: BlockAnnotations,
        source_span: Option<Span>,
    },
    Put {
        block: Block,
        match_annotations: BlockAnnotations,
        insert_annotations: BlockAnnotations,
        source_span: Option<Span>,
    },
    Delete {
        block: Block,
        deleted_variables: Vec<Variable>,
        annotations: BlockAnnotations,
        source_span: Option<Span>,
    },
    // ...
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Require(Require),
    Distinct(Distinct),
    Reduce(Reduce, Vec<ReduceInstruction<Variable>>),
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
    let mut ctx = PipelineAnnotationContext::new(
        snapshot,
        type_manager,
        &combined_signature_annotations,
        variable_registry,
        parameters,
    );
    let input_annotations = RunningVariableAnnotations::from_iterator(zip([].into_iter(), [].into_iter()));
    let (annotated_stages, annotated_fetch) =
        annotate_stages_and_fetch(&mut ctx, translated_stages, translated_fetch, input_annotations)?;
    Ok(AnnotatedPipeline { annotated_stages, annotated_fetch, annotated_preamble })
}

pub(crate) fn annotate_stages_and_fetch(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    translated_stages: Vec<TranslatedStage>,
    translated_fetch: Option<FetchObject>,
    input_annotations: RunningVariableAnnotations,
) -> Result<(Vec<AnnotatedStage>, Option<AnnotatedFetch>), AnnotationError> {
    let (annotated_stages, output_annotations) =
        annotate_pipeline_stages(ctx, translated_stages, input_annotations, None)?;
    let annotated_fetch = match translated_fetch {
        None => None,
        Some(fetch) => {
            let annotated = annotate_fetch(ctx, fetch, &output_annotations);
            Some(annotated?)
        }
    };
    Ok((annotated_stages, annotated_fetch))
}

pub(crate) fn annotate_pipeline_stages(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    translated_stages: Vec<TranslatedStage>,
    input_annotations: RunningVariableAnnotations,
    return_variables: Option<&[Variable]>, // Remove if anonymous vars can't cross stage boundaries
) -> Result<(Vec<AnnotatedStage>, RunningVariableAnnotations), AnnotationError> {
    let mut running_annotations = input_annotations;
    let mut annotated_stages = Vec::with_capacity(translated_stages.len());

    let empty_constraint_annotations = HashMap::new();
    let mut latest_match_index = None;
    for stage in translated_stages {
        let running_constraint_annotations = latest_match_index
            .map(|idx| {
                let AnnotatedStage::Match { block_annotations, block, .. } = annotated_stages.get(idx).unwrap() else {
                    unreachable!("LatestMatchIndex will always be a match");
                };
                block_annotations.type_annotations_of(block.conjunction()).unwrap().constraint_annotations()
            })
            .unwrap_or(&empty_constraint_annotations);
        let annotated_stage = annotate_stage(ctx, &mut running_annotations, running_constraint_annotations, stage)?;

        let retain_running_var_fn =
            |var: &Variable| var.is_named() || return_variables.map(|vars| vars.contains(var)).unwrap_or(false);
        running_annotations.retain(retain_running_var_fn);
        if let AnnotatedStage::Match { .. } = annotated_stage {
            latest_match_index = Some(annotated_stages.len());
        }
        annotated_stages.push(annotated_stage);
    }
    Ok((annotated_stages, running_annotations))
}

fn annotate_stage(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    running_annotations: &mut RunningVariableAnnotations,
    running_constraint_annotations: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    stage: TranslatedStage,
) -> Result<AnnotatedStage, AnnotationError> {
    match stage {
        TranslatedStage::Match { block, source_span } => {
            let mut block_annotations = infer_types(
                ctx.snapshot,
                &block,
                ctx.variable_registry,
                ctx.type_manager,
                &running_annotations.concepts,
                ctx.annotated_function_signatures,
                false,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            let root_annotations = block_annotations.type_annotations_of(block.conjunction()).unwrap();
            root_annotations.vertex_annotations().iter().for_each(|(vertex, types)| {
                if let Some(var) = vertex.as_variable() {
                    running_annotations.concepts.insert(var, types.clone());
                }
            });

            collect_value_types_of_function_call_assignments(ctx, running_annotations, block.conjunction())?;

            // TODO: Why not pass PipelineAnnotationContext?
            let compiled_expressions = compile_expressions(
                ctx.snapshot,
                ctx.type_manager,
                &block,
                ctx.variable_registry,
                ctx.parameters,
                &block_annotations,
                &mut running_annotations.values,
            )
            .map_err(|typedb_source| AnnotationError::ExpressionCompilation { typedb_source })?;
            compiled_expressions.iter().for_each(|(binding, compiled)| {
                let _existing = running_annotations
                    .values
                    .insert(binding.left().as_variable().unwrap(), compiled.return_type().clone());
                debug_assert!(_existing.is_none() || _existing == Some(compiled.return_type().clone()))
            });
            complete_block_annotations_with_value_types(
                ctx,
                &running_annotations,
                block.conjunction(),
                &mut block_annotations,
            )?;
            Ok(AnnotatedStage::Match {
                block,
                block_annotations,
                executable_expressions: compiled_expressions,
                source_span,
            })
        }

        TranslatedStage::Insert { block, source_span } => {
            let annotations = annotate_write_stage(ctx, running_annotations, &block)?;

            check_type_combinations_for_write(
                ctx,
                &block,
                &running_annotations.concepts,
                running_constraint_annotations,
                &annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;

            Ok(AnnotatedStage::Insert { block, annotations, source_span })
        }

        TranslatedStage::Update { block, source_span } => {
            let annotations = annotate_write_stage(ctx, running_annotations, &block)?;

            update::type_check::check_annotations(
                ctx,
                &block,
                &running_annotations.concepts,
                running_constraint_annotations,
                &annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;

            Ok(AnnotatedStage::Update { block, annotations, source_span })
        }

        TranslatedStage::Put { block, source_span } => {
            let mut match_annotations = infer_types(
                ctx.snapshot,
                &block,
                &ctx.variable_registry,
                ctx.type_manager,
                &running_annotations.concepts,
                ctx.annotated_function_signatures,
                false,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            complete_block_annotations_with_value_types(
                ctx,
                &running_annotations,
                block.conjunction(),
                &mut match_annotations,
            )?;
            let insert_annotations = annotate_write_stage(ctx, running_annotations, &block)?;
            check_type_combinations_for_write(
                ctx,
                &block,
                &running_annotations.concepts,
                running_constraint_annotations,
                &insert_annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;

            // Update running annotations based on match annotations as they will be less strict.
            let root_annotations = match_annotations.type_annotations_of(block.conjunction()).unwrap();
            root_annotations.vertex_annotations().iter().for_each(|(vertex, types)| {
                if let Some(var) = vertex.as_variable() {
                    running_annotations.concepts.insert(var, types.clone());
                }
            });

            Ok(AnnotatedStage::Put { block, match_annotations, insert_annotations, source_span })
        }
        TranslatedStage::Delete { block, deleted_variables, source_span } => {
            let delete_annotations = annotate_write_stage(ctx, running_annotations, &block)?;
            check_type_combinations_for_write(
                ctx,
                &block,
                &running_annotations.concepts,
                running_constraint_annotations,
                &delete_annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            let deleted_vars_set: HashSet<Variable> = deleted_variables.iter().copied().collect();
            running_annotations.retain(|var| deleted_vars_set.contains(var));
            Ok(AnnotatedStage::Delete { block, deleted_variables, annotations: delete_annotations, source_span })
        }
        TranslatedStage::Sort(sort) => {
            validate_sort_variables_comparable(ctx, &sort, running_annotations)?;
            Ok(AnnotatedStage::Sort(sort))
        }
        TranslatedStage::Select(select) => {
            running_annotations.retain(|var| select.variables.contains(var));
            Ok(AnnotatedStage::Select(select))
        }
        TranslatedStage::Offset(offset) => Ok(AnnotatedStage::Offset(offset)),
        TranslatedStage::Limit(limit) => Ok(AnnotatedStage::Limit(limit)),
        TranslatedStage::Require(require) => Ok(AnnotatedStage::Require(require)),
        TranslatedStage::Distinct(_) => Ok(AnnotatedStage::Distinct(Distinct)),

        TranslatedStage::Reduce(reduce) => {
            let mut reduce_instructions = Vec::with_capacity(reduce.assigned_reductions.len());
            for &AssignedReduction { assigned, reduction } in &reduce.assigned_reductions {
                let typed_reduce =
                    resolve_reducer_by_value_type(ctx, reduction, running_annotations, reduce.source_span())?;
                running_annotations
                    .values
                    .insert(assigned, ExpressionValueType::Single(typed_reduce.output_type().clone()));
                reduce_instructions.push(typed_reduce);
            }
            Ok(AnnotatedStage::Reduce(reduce, reduce_instructions))
        }
    }
}

fn complete_block_annotations_with_value_types(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    source_running_annotations: &RunningVariableAnnotations,
    conjunction: &Conjunction,
    block_annotations: &mut BlockAnnotations,
) -> Result<(), AnnotationError> {
    let value_types_in_conjunction = conjunction
        .constraints()
        .iter()
        .flat_map(|c| c.ids())
        .filter(|v| ctx.variable_registry.get_variable_category(*v).map_or(false, |cat| cat == VariableCategory::Value))
        .map(|v| {
            (Vertex::Variable(v), source_running_annotations.values.get(&v).expect("Expected value annotation").clone())
        })
        .collect();
    let _existing = block_annotations.set_value_types_of(conjunction, value_types_in_conjunction);
    conjunction.nested_patterns().iter().try_for_each(|pattern| match pattern {
        NestedPattern::Disjunction(disjunction) => disjunction.conjunctions().iter().try_for_each(|c| {
            complete_block_annotations_with_value_types(ctx, source_running_annotations, c, block_annotations)
        }),
        NestedPattern::Negation(inner) => complete_block_annotations_with_value_types(
            ctx,
            source_running_annotations,
            inner.conjunction(),
            block_annotations,
        ),
        NestedPattern::Optional(inner) => complete_block_annotations_with_value_types(
            ctx,
            source_running_annotations,
            inner.conjunction(),
            block_annotations,
        ),
    })
}

pub fn validate_sort_variables_comparable(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    sort: &Sort,
    input_annotations: &RunningVariableAnnotations,
) -> Result<(), AnnotationError> {
    for sort_var in &sort.variables {
        if input_annotations.values.contains_key(&sort_var.variable()) {
            continue; // Expressions always return the same type.
        } else if let Some(types) = input_annotations.concepts.get(&sort_var.variable()) {
            let value_types = resolve_value_types(&(**types), ctx.snapshot, ctx.type_manager)
                .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            if value_types.is_empty() {
                let variable_name = ctx.name_for_error(sort_var.variable());
                return Err(AnnotationError::CouldNotDetermineValueTypeForReducerInput {
                    variable: variable_name,
                    source_span: sort.source_span(),
                });
            }
            let first_category = value_types.iter().find(|_| true).unwrap().category();
            let allowed_categories = ValueTypeCategory::comparable_categories(first_category);
            for other_type in value_types.iter().map(|v| v.category()) {
                // Don't need to do pairwise if comparable is transitive
                if !allowed_categories.contains(&other_type) {
                    let variable_name = ctx.name_for_error(sort_var.variable());
                    return Err(AnnotationError::UncomparableValueTypesForSortVariable {
                        variable: variable_name,
                        category1: first_category,
                        category2: other_type,
                        source_span: sort.source_span(),
                    });
                }
            }
        } else {
            unreachable!()
        }
    }
    Ok(())
}

fn annotate_write_stage(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    running_annotations: &mut RunningVariableAnnotations,
    block: &Block,
) -> Result<BlockAnnotations, AnnotationError> {
    let mut block_annotations = infer_types(
        ctx.snapshot,
        block,
        ctx.variable_registry,
        ctx.type_manager,
        &running_annotations.concepts,
        ctx.annotated_function_signatures,
        true,
    )
    .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;

    complete_block_annotations_with_value_types(
        ctx,
        &running_annotations,
        block.conjunction(),
        &mut block_annotations,
    )?;

    let annotations = block_annotations.type_annotations_of(block.conjunction()).unwrap();

    // Extend running annotations for variables introduced in this stage.
    for constraint in block.conjunction().constraints() {
        annotate_write_constraint(constraint, &mut running_annotations.concepts, annotations)
    }

    for nested_pattern in block.conjunction().nested_patterns() {
        match nested_pattern {
            NestedPattern::Optional(optional) => {
                for constraint in optional.conjunction().constraints() {
                    annotate_write_constraint(constraint, &mut running_annotations.concepts, annotations);
                }
            }
            NestedPattern::Disjunction(_) | NestedPattern::Negation(_) => {
                unreachable!("Non-try nested pattern encountered in a write stage: {nested_pattern}")
            }
        }
    }

    Ok(block_annotations)
}

fn annotate_write_constraint(
    constraint: &Constraint<Variable>,
    running_variable_annotations: &mut BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    annotations: &TypeAnnotations,
) {
    match constraint {
        Constraint::Isa(isa) => {
            running_variable_annotations.insert(
                isa.thing().as_variable().unwrap(),
                annotations.vertex_annotations_of(isa.thing()).unwrap().clone(),
            );
        }
        Constraint::RoleName(role_name) => {
            running_variable_annotations.insert(
                role_name.type_().as_variable().unwrap(),
                annotations.vertex_annotations_of(role_name.type_()).unwrap().clone(),
            );
        }
        Constraint::Links(links) => {
            if let Some(variable) = links.role_type().as_variable() {
                if !running_variable_annotations.contains_key(&variable)
                    && annotations.vertex_annotations_of(links.role_type()).is_some()
                {
                    running_variable_annotations
                        .insert(variable, annotations.vertex_annotations_of(links.role_type()).unwrap().clone());
                }
            }
        }
        _ => (),
    }
}

pub fn resolve_reducer_by_value_type(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    reducer: Reducer,
    variable_annotations: &RunningVariableAnnotations,
    reduce_source_span: Option<Span>,
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
            let value_type =
                determine_value_type_for_reducer(ctx, reducer, variable, variable_annotations, reduce_source_span)?;
            resolve_reduce_instruction_by_value_type(ctx, reducer, value_type, reduce_source_span)
        }
    }
}

fn determine_value_type_for_reducer(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    reducer: Reducer,
    variable: Variable,
    variable_annotations: &RunningVariableAnnotations,
    reduce_source_span: Option<Span>,
) -> Result<ValueType, AnnotationError> {
    if let Some(assigned_type) = variable_annotations.values.get(&variable) {
        match assigned_type {
            ExpressionValueType::Single(value_type) => Ok(value_type.clone()),
            ExpressionValueType::List(_) => {
                let variable_name = ctx.name_for_error(variable);
                Err(AnnotationError::ReducerInputVariableIsList {
                    reducer: reducer.name(),
                    variable: variable_name,
                    source_span: reduce_source_span,
                })
            }
        }
    } else if let Some(types) = variable_annotations.concepts.get(&variable) {
        let value_types = resolve_value_types(types, ctx.snapshot, ctx.type_manager)
            .map_err(|source| AnnotationError::TypeInference { typedb_source: source })?;
        if value_types.len() != 1 {
            let variable_name = ctx.name_for_error(variable);
            Err(AnnotationError::ReducerInputVariableDidNotHaveSingleValueType {
                variable: variable_name,
                source_span: reduce_source_span,
            })
        } else {
            Ok(value_types.iter().next().unwrap().clone())
        }
    } else {
        let variable_name = ctx.name_for_error(variable);
        Err(AnnotationError::CouldNotDetermineValueTypeForReducerInput {
            variable: variable_name,
            source_span: reduce_source_span,
        })
    }
}

fn resolve_reduce_instruction_by_value_type(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    reducer: Reducer,
    value_type: ValueType,
    source_span: Option<Span>,
) -> Result<ReduceInstruction<Variable>, AnnotationError> {
    // Will have been handled earlier since it doesn't need a value type.
    debug_assert!(!matches!(reducer, Reducer::Count) && !matches!(reducer, Reducer::CountVar(_)));

    let err = || {
        let var = reducer.variable().unwrap();
        let reducer_name = reducer.name();
        let variable_name = ctx.name_for_error(var);
        Err(AnnotationError::UnsupportedValueTypeForReducer {
            reducer: reducer_name,
            variable: variable_name,
            value_type: value_type.category(),
            source_span,
        })
    };

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

        ValueTypeCategory::Decimal => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Sum(var) => Ok(ReduceInstruction::SumDecimal(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxDecimal(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinDecimal(var)),
            Reducer::Mean(var) => Ok(ReduceInstruction::MeanDecimal(var)),
            Reducer::Median(var) => Ok(ReduceInstruction::MedianDecimal(var)),
            Reducer::Std(var) => Ok(ReduceInstruction::StdDecimal(var)),
        },

        ValueTypeCategory::String => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxString(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinString(var)),
            _ => err(),
        },

        ValueTypeCategory::Date => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxDate(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinDate(var)),
            _ => err(),
        },

        ValueTypeCategory::DateTime => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxDateTime(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinDateTime(var)),
            _ => err(),
        },

        ValueTypeCategory::DateTimeTZ => match reducer {
            Reducer::Count => Ok(ReduceInstruction::Count),
            Reducer::CountVar(var) => Ok(ReduceInstruction::CountVar(var)),
            Reducer::Max(var) => Ok(ReduceInstruction::MaxDateTimeTZ(var)),
            Reducer::Min(var) => Ok(ReduceInstruction::MinDateTimeTZ(var)),
            _ => err(),
        },

        ValueTypeCategory::Boolean | ValueTypeCategory::Duration | ValueTypeCategory::Struct => err(),
    }
}

fn collect_value_types_of_function_call_assignments(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    running_annotations_to_update: &mut RunningVariableAnnotations,
    conjunction: &Conjunction,
) -> Result<(), AnnotationError> {
    conjunction
        .constraints()
        .iter()
        .filter_map(|constraint| match constraint {
            Constraint::FunctionCallBinding(binding) => Some(binding),
            _ => None,
        })
        .try_for_each(|binding| {
            let return_ = &ctx
                .annotated_function_signatures
                .get_annotated_signature(&binding.function_call().function_id())
                .unwrap()
                .returns;
            zip(binding.assigned(), return_.iter()).try_for_each(|(var, annotation)| match &annotation {
                FunctionParameterAnnotation::Value(value_type) => {
                    if running_annotations_to_update.values.contains_key(&var.as_variable().unwrap()) {
                        let assign_variable = ctx.name_for_error(var.as_variable().unwrap());
                        return Err(AnnotationError::ExpressionCompilation {
                            typedb_source: Box::new(ExpressionCompileError::MultipleAssignmentsForVariable {
                                variable: assign_variable,
                                source_span: binding.source_span(),
                            }),
                        });
                    }
                    running_annotations_to_update
                        .values
                        .insert(var.as_variable().unwrap(), ExpressionValueType::Single(value_type.clone()));
                    Ok(())
                }
                FunctionParameterAnnotation::AnyConcept | FunctionParameterAnnotation::Concept(_) => Ok(()),
            })
        })?;
    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPattern::Disjunction(disjunction) => disjunction.conjunctions().iter().try_for_each(|inner| {
            collect_value_types_of_function_call_assignments(ctx, running_annotations_to_update, inner)
        }),
        NestedPattern::Negation(negation) => {
            collect_value_types_of_function_call_assignments(ctx, running_annotations_to_update, negation.conjunction())
        }
        NestedPattern::Optional(optional) => {
            collect_value_types_of_function_call_assignments(ctx, running_annotations_to_update, optional.conjunction())
        }
    })?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(super) struct RunningVariableAnnotations {
    pub(crate) concepts: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    pub(crate) values: BTreeMap<Variable, ExpressionValueType>,
}

impl RunningVariableAnnotations {
    pub(crate) fn from_iterator(
        iter: impl Iterator<Item = (Variable, FunctionParameterAnnotation)>,
    ) -> RunningVariableAnnotations {
        let mut concepts = BTreeMap::new();
        let mut values = BTreeMap::new();
        iter.for_each(|(var, types)| match types {
            FunctionParameterAnnotation::AnyConcept => unreachable!("Unexpected"),
            FunctionParameterAnnotation::Value(value_type) => {
                needs_update_when_feature_is_implemented!(Lists);
                values.insert(var, ExpressionValueType::Single(value_type.clone()));
            }
            FunctionParameterAnnotation::Concept(types) => {
                concepts.insert(var, Arc::new(types.clone()));
            }
        });
        RunningVariableAnnotations { concepts, values }
    }

    pub(crate) fn retain(&mut self, predicate: impl Fn(&Variable) -> bool) {
        self.concepts.retain(|var, _| predicate(var));
        self.values.retain(|var, _| predicate(var));
    }

    pub(crate) fn get_param(&self, variable: &Variable) -> Option<FunctionParameterAnnotation> {
        needs_update_when_feature_is_implemented!(Lists);
        if let Some(types) = self.concepts.get(&variable) {
            Some(FunctionParameterAnnotation::Concept((**types).clone()))
        } else if let Some(value_type) = self.values.get(&variable) {
            Some(FunctionParameterAnnotation::Value(value_type.value_type().clone()))
        } else {
            None
        }
    }
}
