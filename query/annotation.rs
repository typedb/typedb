/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use compiler::{
    expression::{
        block_compiler::compile_expressions,
        compiled_expression::{CompiledExpression, ExpressionValueType},
    },
    insert::type_check::check_annotations,
    match_::inference::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::{infer_types_for_functions, infer_types_for_match_block},
    },
};
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::constraint::Constraint,
    program::{
        block::{FunctionalBlock, ParameterRegistry, VariableRegistry},
        function::Function,
        modifier::{Limit, Offset, Select, Sort},
    },
};
use ir::program::reduce::Reduce;
use storage::snapshot::ReadableSnapshot;

use crate::{error::QueryError, translation::TranslatedStage};

pub(super) struct AnnotatedPipeline {
    pub(super) annotated_preamble: AnnotatedUnindexedFunctions,
    pub(super) annotated_stages: Vec<AnnotatedStage>,
}

pub(super) enum AnnotatedStage {
    Match {
        block: FunctionalBlock,
        block_annotations: TypeAnnotations,
        compiled_expressions: HashMap<Variable, CompiledExpression>,
    },
    Insert {
        block: FunctionalBlock,
        annotations: TypeAnnotations,
    },
    Delete {
        block: FunctionalBlock,
        deleted_variables: Vec<Variable>,
        annotations: TypeAnnotations,
    },
    // ...
    Filter(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Reduce(Reduce),
}

pub(super) fn infer_types_for_pipeline(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: &IndexedAnnotatedFunctions,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    translated_preamble: Vec<Function>,
    translated_stages: Vec<TranslatedStage>,
) -> Result<AnnotatedPipeline, QueryError> {
    let annotated_preamble =
        infer_types_for_functions(translated_preamble, snapshot, type_manager, schema_function_annotations)
            .map_err(|source| QueryError::FunctionTypeInference { typedb_source: source })?;

    let mut running_variable_annotations: BTreeMap<Variable, Arc<BTreeSet<answer::Type>>> = BTreeMap::new();
    let mut annotated_stages = Vec::with_capacity(translated_stages.len());

    let empty_constraint_annotations = HashMap::new();
    let mut latest_match_index = None;
    for stage in translated_stages {
        let running_constraint_annotations = latest_match_index
            .map(|idx| {
                let AnnotatedStage::Match { block_annotations, .. } = annotated_stages.get(idx).unwrap() else {
                    unreachable!();
                };
                block_annotations.constraint_annotations()
            })
            .unwrap_or(&empty_constraint_annotations);
        let annotated_stage = annotate_stage(
            &mut running_variable_annotations,
            variable_registry,
            parameters,
            snapshot,
            type_manager,
            schema_function_annotations,
            &annotated_preamble,
            running_constraint_annotations,
            stage,
        )?;
        if let AnnotatedStage::Match { .. } = annotated_stage {
            latest_match_index = Some(annotated_stages.len());
        }
        annotated_stages.push(annotated_stage);
    }
    Ok(AnnotatedPipeline { annotated_stages, annotated_preamble })
}

fn annotate_stage(
    running_variable_annotations: &mut BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    variable_registry: &mut VariableRegistry,
    parameters: &ParameterRegistry,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: &IndexedAnnotatedFunctions,
    preamble_function_annotations: &AnnotatedUnindexedFunctions,
    running_constraint_annotations: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    stage: TranslatedStage,
) -> Result<AnnotatedStage, QueryError> {
    match stage {
        TranslatedStage::Match { block } => {
            let block_annotations = infer_types_for_match_block(
                &block,
                variable_registry,
                snapshot,
                type_manager,
                running_variable_annotations,
                schema_function_annotations,
                preamble_function_annotations,
            )
            .map_err(|source| QueryError::QueryTypeInference { typedb_source: source })?;
            block_annotations.vertex_annotations().iter().for_each(|(k, v)| {
                if let Some(k) = k.as_variable() {
                    running_variable_annotations.insert(k, v.clone());
                }
            });
            let compiled_expressions =
                compile_expressions(snapshot, type_manager, &block, variable_registry, parameters, &block_annotations)
                    .map_err(|source| QueryError::ExpressionCompilation { source })?;
            Ok(AnnotatedStage::Match { block, block_annotations, compiled_expressions })
        }
        TranslatedStage::Insert { block } => {
            let insert_annotations = infer_types_for_match_block(
                &block,
                variable_registry,
                snapshot,
                type_manager,
                running_variable_annotations,
                &IndexedAnnotatedFunctions::empty(),
                &AnnotatedUnindexedFunctions::empty(),
            )
            .map_err(|source| QueryError::QueryTypeInference { typedb_source: source })?;
            block.conjunction().constraints().iter().for_each(|constraint| match constraint {
                Constraint::Isa(isa) => {
                    running_variable_annotations.insert(
                        isa.thing().as_variable().unwrap(),
                        insert_annotations.vertex_annotations_of(isa.thing()).unwrap().clone(),
                    );
                }
                Constraint::RoleName(role_name) => {
                    running_variable_annotations.insert(
                        role_name.left().as_variable().unwrap(),
                        insert_annotations.vertex_annotations_of(role_name.left()).unwrap().clone(),
                    );
                }
                _ => {}
            });

            check_annotations(
                snapshot,
                type_manager,
                &block,
                running_variable_annotations,
                running_constraint_annotations,
                &insert_annotations,
            )
            .map_err(|source| QueryError::QueryTypeInference { typedb_source: source })?;

            Ok(AnnotatedStage::Insert { block, annotations: insert_annotations })
        }
        TranslatedStage::Delete { block, deleted_variables } => {
            let delete_annotations = infer_types_for_match_block(
                &block,
                variable_registry,
                snapshot,
                type_manager,
                running_variable_annotations,
                &IndexedAnnotatedFunctions::empty(),
                &AnnotatedUnindexedFunctions::empty(),
            )
            .map_err(|source| QueryError::QueryTypeInference { typedb_source: source })?;
            deleted_variables.iter().for_each(|v| {
                running_variable_annotations.remove(v);
            });
            // TODO: check_annotations on deletes. Can only delete links or has for types that actually are linked or owned
            Ok(AnnotatedStage::Delete { block, deleted_variables, annotations: delete_annotations })
        }
        TranslatedStage::Sort(sort) => Ok(AnnotatedStage::Sort(sort)),
        TranslatedStage::Filter(select) => Ok(AnnotatedStage::Filter(select)),
        TranslatedStage::Offset(offset) => Ok(AnnotatedStage::Offset(offset)),
        TranslatedStage::Limit(limit) => Ok(AnnotatedStage::Limit(limit)),
        TranslatedStage::Reduce(reduce) => Ok(AnnotatedStage::Reduce(reduce)),
    }
}
