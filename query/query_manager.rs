/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::{
    annotation::pipeline::{annotate_pipeline, AnnotatedPipeline},
    VariablePosition,
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    delete::DeleteStageExecutor,
    initial::InitialStage,
    insert::InsertStageExecutor,
    match_::MatchStageExecutor,
    modifiers::{
        LimitStageExecutor, OffsetStageExecutor, RequireStageExecutor, SelectStageExecutor, SortStageExecutor,
    },
    reduce::ReduceStageExecutor,
    stage::{ExecutionContext, ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    pipeline::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;
use compiler::executable::pipeline::{compile_pipeline, ExecutablePipeline, ExecutableStage};

use crate::{
    define,
    error::QueryError,
    redefine, undefine,
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
        // 1: Translate
        let TranslatedPipeline { translated_preamble, translated_stages, mut variable_registry, value_parameters } =
            self.translate_pipeline(snapshot.as_ref(), function_manager, query)?;

        // 2: Annotate
        let annotated_functions = function_manager
            .get_annotated_functions(snapshot.as_ref(), &type_manager)
            .map_err(|err| QueryError::FunctionRetrieval { typedb_source: err })?;

        let AnnotatedPipeline { annotated_preamble, annotated_stages } = annotate_pipeline(
            snapshot.as_ref(),
            type_manager,
            &annotated_functions,
            &mut variable_registry,
            &value_parameters,
            translated_preamble,
            translated_stages,
        )
        .map_err(|err| QueryError::Annotation { typedb_source: err })?;

        // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let ExecutablePipeline { executable_functions: compiled_functions, executable_stages: compiled_stages, stages_variable_positions: output_variable_positions } = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            annotated_preamble,
            annotated_stages,
        )?;

        let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(value_parameters));
        let mut last_stage = ReadPipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage = MatchStageExecutor::new(match_executable, last_stage);
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(_) => {
                    unreachable!("Insert clause cannot exist in a read pipeline.")
                }
                ExecutableStage::Delete(_) => {
                    unreachable!("Delete clause cannot exist in a read pipeline.")
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable, last_stage);
                    last_stage = ReadPipelineStage::Select(Box::new(select_stage));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable, last_stage);
                    last_stage = ReadPipelineStage::Sort(Box::new(sort_stage));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable, last_stage);
                    last_stage = ReadPipelineStage::Offset(Box::new(offset_stage));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable, last_stage);
                    last_stage = ReadPipelineStage::Limit(Box::new(limit_stage));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable, last_stage);
                    last_stage = ReadPipelineStage::Require(Box::new(require_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable, last_stage);
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
        let TranslatedPipeline { translated_preamble, translated_stages, mut variable_registry, value_parameters } =
            match self.translate_pipeline(&snapshot, function_manager, query) {
                Ok(translated) => translated,
                Err(err) => return Err((snapshot, err)),
            };

        // 2: Annotate
        let annotated_functions = match function_manager.get_annotated_functions(&snapshot, type_manager) {
            Ok(functions) => functions,
            Err(err) => {
                return Err((snapshot, QueryError::FunctionRetrieval { typedb_source: err }));
            }
        };

        let annotated_pipeline = annotate_pipeline(
            &snapshot,
            type_manager,
            &annotated_functions,
            &mut variable_registry,
            &value_parameters,
            translated_preamble,
            translated_stages,
        );

        let AnnotatedPipeline { annotated_preamble, annotated_stages } = match annotated_pipeline {
            Ok(annotated_pipeline) => annotated_pipeline,
            Err(err) => return Err((snapshot, QueryError::Annotation { typedb_source: err })),
        };

        // // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let executable_pipeline = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            annotated_preamble,
            annotated_stages,
        );
        let ExecutablePipeline { executable_functions, executable_stages, stages_variable_positions: output_variable_positions } =
            match executable_pipeline {
                Ok(executable) => executable,
                Err(err) => return Err((snapshot, err)),
            };

        let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::new(value_parameters));
        let mut previous_stage = WritePipelineStage::Initial(InitialStage::new(context));
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage = MatchStageExecutor::new(match_executable, previous_stage);
                    previous_stage = WritePipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(insert_executable) => {
                    let insert_stage = InsertStageExecutor::new(insert_executable, previous_stage);
                    previous_stage = WritePipelineStage::Insert(Box::new(insert_stage));
                }
                ExecutableStage::Delete(delete_executable) => {
                    let delete_stage = DeleteStageExecutor::new(delete_executable, previous_stage);
                    previous_stage = WritePipelineStage::Delete(Box::new(delete_stage));
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable, previous_stage);
                    previous_stage = WritePipelineStage::Select(Box::new(select_stage));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable, previous_stage);
                    previous_stage = WritePipelineStage::Sort(Box::new(sort_stage));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable, previous_stage);
                    previous_stage = WritePipelineStage::Offset(Box::new(offset_stage));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable, previous_stage);
                    previous_stage = WritePipelineStage::Limit(Box::new(limit_stage));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable, previous_stage);
                    previous_stage = WritePipelineStage::Require(Box::new(require_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable, previous_stage);
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

    fn translate_pipeline<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<TranslatedPipeline, QueryError> {
        let preamble_signatures = HashMapFunctionSignatureIndex::build(
            query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
        );
        let all_function_signatures =
            ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
        translate_pipeline(snapshot, &all_function_signatures, query)
            .map_err(|err| QueryError::Representation { typedb_source: err })
    }
}
