/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    delete::DeleteStageExecutor,
    initial::InitialStage,
    insert::InsertStageExecutor,
    match_::MatchStageExecutor,
    modifiers::{LimitStageExecutor, OffsetStageExecutor, SelectStageExecutor, SortStageExecutor},
    stage::{ExecutionContext, ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::FunctionManager;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;
use executor::pipeline::reduce::ReduceStageExecutor;

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
    ) -> Result<(ReadPipelineStage<Snapshot>, HashMap<String, VariablePosition>), QueryError> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        // 1: Translate
        let TranslatedPipeline { translated_preamble, translated_stages, mut variable_registry, parameters } =
            translate_pipeline(snapshot.as_ref(), function_manager, query)?;

        let annotated_functions = function_manager
            .get_annotated_functions(snapshot.as_ref(), &type_manager)
            .map_err(|err| QueryError::FunctionRetrieval { typedb_source: err })?;

        // 2: Annotate
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = infer_types_for_pipeline(
            snapshot.as_ref(),
            type_manager,
            &annotated_functions,
            &mut variable_registry,
            &parameters,
            translated_preamble,
            translated_stages,
        )?;
        // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let CompiledPipeline { compiled_functions, compiled_stages, output_variable_positions } = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            annotated_preamble,
            annotated_stages,
        )?;

        let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(parameters));
        let mut last_stage = ReadPipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_program) => {
                    // TODO: Pass expressions & functions
                    // let program_plan = ProgramPlan::new(match_program, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(match_program, last_stage);
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                CompiledStage::Insert(_) => {
                    unreachable!("Insert clause cannot exist in a read pipeline.")
                }
                CompiledStage::Delete(_) => {
                    unreachable!("Delete clause cannot exist in a read pipeline.")
                }
                CompiledStage::Filter(filter_program) => {
                    let filter_stage = SelectStageExecutor::new(filter_program, last_stage);
                    last_stage = ReadPipelineStage::Select(Box::new(filter_stage));
                }
                CompiledStage::Sort(sort_program) => {
                    let sort_stage = SortStageExecutor::new(sort_program, last_stage);
                    last_stage = ReadPipelineStage::Sort(Box::new(sort_stage));
                }
                CompiledStage::Offset(offset_program) => {
                    let offset_stage = OffsetStageExecutor::new(offset_program, last_stage);
                    last_stage = ReadPipelineStage::Offset(Box::new(offset_stage));
                }
                CompiledStage::Limit(limit_program) => {
                    let limit_stage = LimitStageExecutor::new(limit_program, last_stage);
                    last_stage = ReadPipelineStage::Limit(Box::new(limit_stage));
                }
                CompiledStage::Reduce(reduce_program) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_program, last_stage);
                    last_stage = ReadPipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }

        let named_outputs = output_variable_positions
            .iter()
            .filter_map(|(variable, &position)| {
                variable_registry.variable_names().get(variable).map(|name| (name.clone(), position))
            })
            .collect::<HashMap<_, _>>();
        Ok((last_stage, named_outputs))
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<(WritePipelineStage<Snapshot>, HashMap<String, VariablePosition>), (Snapshot, QueryError)> {
        // 1: Translate
        let translated_pipeline = translate_pipeline(&snapshot, function_manager, query);
        let TranslatedPipeline { translated_preamble, translated_stages, mut variable_registry, parameters } =
            match translated_pipeline {
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
            &mut variable_registry,
            &parameters,
            translated_preamble,
            translated_stages,
        );
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = match annotated_pipeline {
            Ok(annotated_pipeline) => annotated_pipeline,
            Err(err) => return Err((snapshot, err)),
        };

        // // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let compiled_pipeline = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            annotated_preamble,
            annotated_stages,
        );
        let CompiledPipeline { compiled_functions, compiled_stages, output_variable_positions } =
            match compiled_pipeline {
                Ok(compiled_pipeline) => compiled_pipeline,
                Err(err) => return Err((snapshot, err)),
            };

        let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::new(parameters));
        let mut previous_stage = WritePipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_program) => {
                    // TODO: Pass expressions & functions
                    // let program_plan = ProgramPlan::new(match_program, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(match_program, previous_stage);
                    previous_stage = WritePipelineStage::Match(Box::new(match_stage));
                }
                CompiledStage::Insert(insert_program) => {
                    let insert_stage = InsertStageExecutor::new(insert_program, previous_stage);
                    previous_stage = WritePipelineStage::Insert(Box::new(insert_stage));
                }
                CompiledStage::Delete(delete_program) => {
                    let delete_stage = DeleteStageExecutor::new(delete_program, previous_stage);
                    previous_stage = WritePipelineStage::Delete(Box::new(delete_stage));
                }
                CompiledStage::Filter(filter_program) => {
                    let filter_stage = SelectStageExecutor::new(filter_program, previous_stage);
                    previous_stage = WritePipelineStage::Select(Box::new(filter_stage));
                }
                CompiledStage::Sort(sort_program) => {
                    let sort_stage = SortStageExecutor::new(sort_program, previous_stage);
                    previous_stage = WritePipelineStage::Sort(Box::new(sort_stage));
                }
                CompiledStage::Offset(offset_program) => {
                    let offset_stage = OffsetStageExecutor::new(offset_program, previous_stage);
                    previous_stage = WritePipelineStage::Offset(Box::new(offset_stage));
                }
                CompiledStage::Limit(limit_program) => {
                    let limit_stage = LimitStageExecutor::new(limit_program, previous_stage);
                    previous_stage = WritePipelineStage::Limit(Box::new(limit_stage));
                }
                CompiledStage::Reduce(reduce_program) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_program, previous_stage);
                    previous_stage = WritePipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }

        let named_outputs = output_variable_positions
            .iter()
            .filter_map(|(variable, &position)| {
                variable_registry.variable_names().get(variable).map(|name| (name.clone(), position))
            })
            .collect::<HashMap<_, _>>();
        Ok((previous_stage, named_outputs))
    }
}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}
