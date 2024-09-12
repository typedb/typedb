/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::{
    pipeline::{
        delete::DeleteStageExecutor,
        initial::InitialStage,
        insert::InsertStageExecutor,
        match_::MatchStageExecutor,
        stage::{ReadPipelineStage, WritePipelineStage},
    },
    write::{delete::DeleteExecutor, insert::InsertExecutor},
};
use function::function_manager::FunctionManager;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;

use crate::{
    annotation::{infer_types_for_pipeline, AnnotatedPipeline},
    compilation::{compile_pipeline, CompiledPipeline, CompiledStage},
    define,
    error::QueryError,
    redefine,
    translation::{translate_pipeline, TranslatedPipeline},
    undefine,
};

pub struct QueryManager {}

impl QueryManager {
    // TODO: clean up if QueryManager remains stateless
    pub fn new() -> QueryManager {
        QueryManager {}
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        query: SchemaQuery,
    ) -> Result<(), QueryError> {
        match query {
            SchemaQuery::Define(define) => define::execute(snapshot, type_manager, thing_manager, define)
                .map_err(|err| QueryError::Define { typedb_source: err }),
            SchemaQuery::Redefine(redefine) => redefine::execute(snapshot, type_manager, thing_manager, redefine)
                .map_err(|err| QueryError::Redefine { typedb_source: err }),
            SchemaQuery::Undefine(undefine) => undefine::execute(snapshot, type_manager, thing_manager, undefine)
                .map_err(|err| QueryError::Undefine { typedb_source: err }),
        }
    }

    pub fn prepare_read_pipeline<Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<ReadPipelineStage<Snapshot>, QueryError> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        let mut snapshot = snapshot;
        // 1: Translate
        let TranslatedPipeline { translated_preamble, translated_stages, variable_registry } =
            translate_pipeline(snapshot.as_ref(), function_manager, query)?;

        let annotated_functions = function_manager
            .get_annotated_functions(snapshot.as_ref(), &type_manager)
            .map_err(|err| QueryError::FunctionRetrieval { typedb_source: err })?;

        // 2: Annotate
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = infer_types_for_pipeline(
            snapshot.as_ref(),
            type_manager,
            &annotated_functions,
            &variable_registry,
            translated_preamble,
            translated_stages,
        )?;
        // // TODO: Improve how we do this. This is a temporary workaround
        // annotated_stages.iter().filter_map(|stage| {
        //     if let AnnotatedStage::Match { block, variable_value_types, .. }  = stage {
        //         Some((block, variable_value_types))
        //     } else { None }
        // }).try_for_each(|(block, expr) | {
        //     expr.iter().try_for_each(|(var, type_)| {
        //         // TODO: May be in a nested pattern
        //         let source = block.conjunction().constraints().iter().find(|constraint| Some(var.clone()) == constraint.as_expression_binding().map(|expr| expr.left())).unwrap().clone();
        //         let category = match type_ {
        //             ExpressionValueType::Single(_) => VariableCategory::Value,
        //             ExpressionValueType::List(_) => VariableCategory::ValueList,
        //         };
        //         variable_registry.set_assigned_value_variable_category(var.clone(), category, source)?;
        //         Ok::<(), PatternDefinitionError>(())
        //     })
        // }).unwrap();

        // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let CompiledPipeline { compiled_functions, compiled_stages } =
            compile_pipeline(thing_manager.statistics(), variable_registry, annotated_preamble, annotated_stages)?;

        let mut last_stage = ReadPipelineStage::Initial(InitialStage::new(snapshot));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_program) => {
                    // TODO: Pass expressions & functions
                    // let program_plan = ProgramPlan::new(match_program, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(match_program, last_stage, thing_manager.clone());
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                CompiledStage::Insert(_) => {
                    unreachable!("Insert clause cannot exist in a read pipeline.")
                }
                CompiledStage::Delete(_) => {
                    unreachable!("Delete clause cannot exist in a read pipeline.")
                }
            }
        }
        Ok(last_stage)
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<WritePipelineStage<Snapshot>, (Snapshot, QueryError)> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        // 1: Translate
        let translated_pipeline = translate_pipeline(&snapshot, function_manager, query);
        let TranslatedPipeline { translated_preamble, translated_stages, variable_registry } = match translated_pipeline
        {
            Ok(translated_pipeline) => translated_pipeline,
            Err(err) => return Err((snapshot, err)),
        };

        let annotated_functions = match function_manager.get_annotated_functions(&snapshot, &type_manager) {
            Ok(annotated_functions) => annotated_functions,
            Err(err) => return Err((snapshot, QueryError::FunctionRetrieval { typedb_source: err })),
        };

        // 2: Annotate
        let annotated_pipeline = infer_types_for_pipeline(
            &snapshot,
            type_manager,
            &annotated_functions,
            &variable_registry,
            translated_preamble,
            translated_stages,
        );
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = match annotated_pipeline {
            Ok(annotated_pipeline) => annotated_pipeline,
            Err(err) => return Err((snapshot, err)),
        };
        // // TODO: Improve how we do this. This is a temporary workaround
        // annotated_stages.iter().filter_map(|stage| {
        //     if let AnnotatedStage::Match { block, variable_value_types, .. }  = stage {
        //         Some((block, variable_value_types))
        //     } else { None }
        // }).try_for_each(|(block, expr) | {
        //     expr.iter().try_for_each(|(var, type_)| {
        //         // TODO: May be in a nested pattern
        //         let source = block.conjunction().constraints().iter().find(|constraint| Some(var.clone()) == constraint.as_expression_binding().map(|expr| expr.left())).unwrap().clone();
        //         let category = match type_ {
        //             ExpressionValueType::Single(_) => VariableCategory::Value,
        //             ExpressionValueType::List(_) => VariableCategory::ValueList,
        //         };
        //         variable_registry.set_assigned_value_variable_category(var.clone(), category, source)?;
        //         Ok::<(), PatternDefinitionError>(())
        //     })
        // }).unwrap();

        // // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let compiled_pipeline =
            compile_pipeline(thing_manager.statistics(), variable_registry, annotated_preamble, annotated_stages);
        let CompiledPipeline { compiled_functions, compiled_stages } = match compiled_pipeline {
            Ok(compiled_pipeline) => compiled_pipeline,
            Err(err) => return Err((snapshot, err)),
        };

        let mut last_stage = WritePipelineStage::Initial(InitialStage::new(Arc::new(snapshot)));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_program) => {
                    // TODO: Pass expressions & functions
                    // let program_plan = ProgramPlan::new(match_program, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(match_program, last_stage, thing_manager.clone());
                    last_stage = WritePipelineStage::Match(Box::new(match_stage));
                }
                CompiledStage::Insert(insert_program) => {
                    let insert_stage = InsertStageExecutor::new(
                        InsertExecutor::new(insert_program),
                        last_stage,
                        thing_manager.clone(),
                    );
                    last_stage = WritePipelineStage::Insert(Box::new(insert_stage));
                }
                CompiledStage::Delete(delete_program) => {
                    let delete_stage = DeleteStageExecutor::new(
                        DeleteExecutor::new(delete_program),
                        last_stage,
                        thing_manager.clone(),
                    );
                    last_stage = WritePipelineStage::Delete(Box::new(delete_stage));
                }
            }
        }
        Ok(last_stage)
    }
}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}
