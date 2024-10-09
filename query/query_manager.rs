/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::{
    annotation::pipeline::{annotate_pipeline, AnnotatedPipeline},
    executable::match_::planner::match_executable::MatchExecutable,
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
    program::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;
use compiler::executable::match_::planner::program_executable::ProgramExecutable;

use crate::{
    compilation::{compile_pipeline, CompiledPipeline, CompiledStage},
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
        let CompiledPipeline { compiled_functions, compiled_stages, output_variable_positions } = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            annotated_preamble,
            annotated_stages,
        )?;

        let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(value_parameters));
        let mut last_stage = ReadPipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let program_plan = ProgramExecutable::new(match_executable, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(program_plan, last_stage);
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                CompiledStage::Insert(_) => {
                    unreachable!("Insert clause cannot exist in a read pipeline.")
                }
                CompiledStage::Delete(_) => {
                    unreachable!("Delete clause cannot exist in a read pipeline.")
                }
                CompiledStage::Select(select_program) => {
                    let select_stage = SelectStageExecutor::new(select_program, last_stage);
                    last_stage = ReadPipelineStage::Select(Box::new(select_stage));
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
                CompiledStage::Require(require_program) => {
                    let require_stage = RequireStageExecutor::new(require_program, last_stage);
                    last_stage = ReadPipelineStage::Require(Box::new(require_stage));
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

        let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::new(value_parameters));
        let mut previous_stage = WritePipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let program_executable = ProgramExecutable::new(match_executable, HashMap::new(), HashMap::new());
                    let match_stage = MatchStageExecutor::new(program_executable, previous_stage);
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
                CompiledStage::Select(select_program) => {
                    let select_stage = SelectStageExecutor::new(select_program, previous_stage);
                    previous_stage = WritePipelineStage::Select(Box::new(select_stage));
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
                CompiledStage::Require(require_program) => {
                    let require_stage = RequireStageExecutor::new(require_program, previous_stage);
                    previous_stage = WritePipelineStage::Require(Box::new(require_stage));
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
