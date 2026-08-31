/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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
        constraint::{Constraint, DeleteConcepts},
        nested_pattern::NestedPattern,
        variable_category::VariableOptionality,
    },
    pipeline::{
        ParameterRegistry, VariableRegistry,
        block::Block,
        fetch::FetchObject,
        function::Function,
        modifier::{Distinct, Limit, Offset, Require, Select, Sort},
        reduce::{AssignedReduction, Reduce, Reducer},
    },
    translation::pipeline::{TranslatedGiven, TranslatedStage},
};
use storage::snapshot::ReadableSnapshot;
use typeql::{common::Span, type_::NamedTypeAny};

use crate::{
    PipelineOrigin,
    annotation::{
        AnnotationError, PipelineAnnotationContext,
        expression::compiled_expression::ExpressionValueType,
        fetch::{AnnotatedFetch, annotate_fetch},
        function::{
            AnnotatedFunctionSignaturesImpl, AnnotatedPreambleFunctions, AnnotatedSchemaFunctions,
            FunctionParameterAnnotation, annotate_preamble_functions, get_annotations_from_labels_vec,
        },
        inference::match_inference::infer_types_for_block,
        type_annotations::{BlockAnnotations, ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::{TypeInferenceMode, resolve_value_types},
        write_type_check::check_type_combinations_for_write,
    },
    executable::{reduce::ReduceInstruction, update},
};

pub struct AnnotatedGiven {
    pub variables: Vec<Variable>,
    pub expected_types: Vec<FunctionParameterAnnotation>,
    pub optionality: Vec<VariableOptionality>,
}

pub struct AnnotatedPipeline {
    pub annotated_preamble: AnnotatedPreambleFunctions,
    pub annotated_given: Option<AnnotatedGiven>,
    pub annotated_stages: Vec<AnnotatedStage>,
    pub annotated_fetch: Option<AnnotatedFetch>,
}

#[derive(Debug, Clone)]
pub enum AnnotatedStage {
    Match {
        block: Block,
        block_annotations: BlockAnnotations,
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
    translated_given: Option<TranslatedGiven>,
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
    let annotated_given = annotate_given_stage(&mut ctx, translated_given)?;
    let input_annotations = if let Some(given) = &annotated_given {
        RunningVariableAnnotations::from_iterator(
            given.variables.iter().copied().zip(given.expected_types.iter().cloned()),
        )
    } else {
        RunningVariableAnnotations::empty()
    };
    let (annotated_stages, output_annotations) =
        annotate_pipeline_stages(&mut ctx, translated_stages, input_annotations, None, PipelineOrigin::Query)?;
    let annotated_fetch =
        translated_fetch.map(|fetch| annotate_fetch(&mut ctx, fetch, &output_annotations)).transpose()?;
    Ok(AnnotatedPipeline { annotated_given, annotated_stages, annotated_fetch, annotated_preamble })
}

fn annotate_given_stage(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    translated_given: Option<TranslatedGiven>,
) -> Result<Option<AnnotatedGiven>, AnnotationError> {
    let Some(TranslatedGiven { variables, labels, .. }) = translated_given else {
        return Ok(None);
    };
    let expected_types = get_annotations_from_labels_vec(&ctx.to_parts_mut().0, &labels).map_err(
        |(index, source_span, typedb_source)| AnnotationError::CouldNotResolveGivenRowDeclaredType {
            index,
            source_span,
            typedb_source,
        },
    )?;
    let optionality = variables
        .iter()
        .zip(labels.iter())
        .map(|(&variable, label)| {
            if matches!(label, NamedTypeAny::Optional(_)) {
                VariableOptionality::Optional
            } else {
                VariableOptionality::Required
            }
        })
        .collect();
    Ok(Some(AnnotatedGiven { variables, expected_types, optionality }))
}

pub(crate) fn annotate_pipeline_stages(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    translated_stages: Vec<TranslatedStage>,
    input_annotations: RunningVariableAnnotations,
    return_variables: Option<&[Variable]>,
    pipeline_origin: PipelineOrigin,
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
        let annotated_stage =
            annotate_stage(ctx, &mut running_annotations, running_constraint_annotations, stage, pipeline_origin)?;

        // running_annotations.retain(|var| var.is_named());
        let retain_running_var_fn =
            |var: &Variable| var.is_named() || return_variables.as_ref().map_or(false, |vars| vars.contains(var));
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
    pipeline_origin: PipelineOrigin,
) -> Result<AnnotatedStage, AnnotationError> {
    match stage {
        TranslatedStage::Match { block, source_span } => {
            let type_inference_mode = match pipeline_origin {
                PipelineOrigin::Schema => TypeInferenceMode::IncludeAbstractSubtypes,
                PipelineOrigin::Query => TypeInferenceMode::ConcreteSubtypesOnly,
            };
            let block_annotations = infer_types_for_block(ctx, &running_annotations, &block, type_inference_mode)
                .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            let root_annotations = block_annotations.type_annotations_of(block.conjunction()).unwrap();
            running_annotations.update_with(root_annotations);
            Ok(AnnotatedStage::Match { block, block_annotations, source_span })
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
            debug_assert!(pipeline_origin == PipelineOrigin::Query);
            let match_annotations =
                infer_types_for_block(ctx, running_annotations, &block, TypeInferenceMode::ConcreteSubtypesOnly)
                    .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
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
            running_annotations.update_with(root_annotations);

            Ok(AnnotatedStage::Put { block, match_annotations, insert_annotations, source_span })
        }
        TranslatedStage::Delete { block, source_span } => {
            let mut delete_annotations = annotate_write_stage(ctx, running_annotations, &block)?;
            check_type_combinations_for_write(
                ctx,
                &block,
                &running_annotations.concepts,
                running_constraint_annotations,
                &delete_annotations,
            )
            .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;
            collect_deleted_variables(&block).iter().for_each(|var| {
                running_annotations.concepts.remove(var);
            });
            Ok(AnnotatedStage::Delete { block, annotations: delete_annotations, source_span })
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
            let first_category = value_types.iter().next().unwrap().category();
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
    let block_annotations = infer_types_for_block(ctx, running_annotations, block, TypeInferenceMode::ExactAndExplicit)
        .map_err(|typedb_source| AnnotationError::TypeInference { typedb_source })?;

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

pub fn collect_deleted_variables(block: &Block) -> BTreeSet<Variable> {
    fn collect_recursive(conjunction: &Conjunction, deleted_variables: &mut BTreeSet<Variable>) {
        for delete_concepts in conjunction.constraints().iter().filter_map(|c| c.as_delete_concepts()) {
            deleted_variables.extend(delete_concepts.ids())
        }
        for inner_conjunction in conjunction.nested_patterns_flattened() {
            collect_recursive(inner_conjunction, deleted_variables);
        }
    }
    let mut deleted_variables = BTreeSet::new();
    collect_recursive(block.conjunction(), &mut deleted_variables);
    deleted_variables
}

#[derive(Debug, Clone)]
pub struct RunningVariableAnnotations {
    pub(crate) concepts: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    pub(crate) values: BTreeMap<Variable, ExpressionValueType>,
}

impl RunningVariableAnnotations {
    pub fn empty() -> Self {
        Self { concepts: BTreeMap::new(), values: BTreeMap::new() }
    }

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

    pub(crate) fn update_with(&mut self, stage_root_annotations: &TypeAnnotations) {
        stage_root_annotations.vertex_annotations().iter().for_each(|(vertex, types)| {
            if let Some(var) = vertex.as_variable() {
                self.concepts.insert(var, types.clone());
            }
        });
        stage_root_annotations.value_annotations().iter().for_each(|(vertex, types)| {
            if let Some(var) = vertex.as_variable() {
                self.values.insert(var, types.clone());
            }
        });
    }

    pub(crate) fn retain(&mut self, predicate: impl Fn(&Variable) -> bool) {
        self.concepts.retain(|var, _| predicate(var));
        self.values.retain(|var, _| predicate(var));
    }

    pub(crate) fn get_as_parameter(&self, variable: &Variable) -> Option<FunctionParameterAnnotation> {
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
