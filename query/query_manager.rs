/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use compiler::{
    annotation::pipeline::{annotate_pipeline, AnnotatedPipeline},
    executable::pipeline::{compile_pipeline, ExecutablePipeline},
};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::pipeline::{
    pipeline::Pipeline,
    stage::{ReadPipelineStage, WritePipelineStage},
};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    pipeline::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    translation::pipeline::{translate_pipeline, TranslatedPipeline},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::SchemaQuery;

use crate::{define, error::QueryError, redefine, undefine};

#[derive(Default)]
pub struct QueryManager {}

impl QueryManager {
    // TODO: clean up if QueryManager remains stateless
    pub fn new() -> Self {
        Self::default()
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
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, QueryError> {
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters: parameters,
        } = self.translate_pipeline(snapshot.as_ref(), function_manager, query)?;

        // 2: Annotate
        let annotated_schema_functions = function_manager
            .get_annotated_functions(snapshot.as_ref(), type_manager)
            .map_err(|err| QueryError::FunctionRetrieval { typedb_source: err })?;

        let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = annotate_pipeline(
            snapshot.as_ref(),
            type_manager,
            &annotated_schema_functions,
            &mut variable_registry,
            &parameters,
            translated_preamble,
            translated_stages,
            translated_fetch,
        )
        .map_err(|err| QueryError::Annotation { typedb_source: err })?;

        // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch } = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            &annotated_schema_functions,
            annotated_preamble,
            annotated_stages,
            annotated_fetch,
            &HashSet::with_capacity(0),
        )
        .map_err(|err| QueryError::ExecutableCompilation { typedb_source: err })?;

        // 4: Executor
        Ok(Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.as_ref(),
            Arc::new(executable_functions),
            &executable_stages,
            executable_fetch,
            Arc::new(parameters),
            None,
        ))
    }

    pub fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, WritePipelineStage<Snapshot>>, (Snapshot, QueryError)> {
        // 1: Translate
        let TranslatedPipeline {
            translated_preamble,
            translated_stages,
            translated_fetch,
            mut variable_registry,
            value_parameters,
        } = match self.translate_pipeline(&snapshot, function_manager, query) {
            Ok(translated) => translated,
            Err(err) => return Err((snapshot, err)),
        };

        // 2: Annotate
        let annotated_schema_functions = match function_manager.get_annotated_functions(&snapshot, type_manager) {
            Ok(functions) => functions,
            Err(err) => {
                return Err((snapshot, QueryError::FunctionRetrieval { typedb_source: err }));
            }
        };

        let annotated_pipeline = annotate_pipeline(
            &snapshot,
            type_manager,
            &annotated_schema_functions,
            &mut variable_registry,
            &value_parameters,
            translated_preamble,
            translated_stages,
            translated_fetch,
        );

        let AnnotatedPipeline { annotated_preamble, annotated_stages, annotated_fetch } = match annotated_pipeline {
            Ok(annotated_pipeline) => annotated_pipeline,
            Err(err) => return Err((snapshot, QueryError::Annotation { typedb_source: err })),
        };

        // 3: Compile
        let variable_registry = Arc::new(variable_registry);
        let executable_pipeline = compile_pipeline(
            thing_manager.statistics(),
            variable_registry.clone(),
            &annotated_schema_functions,
            annotated_preamble,
            annotated_stages,
            annotated_fetch,
            &HashSet::with_capacity(0),
        );
        let ExecutablePipeline { executable_functions, executable_stages, executable_fetch } = match executable_pipeline
        {
            Ok(executable) => executable,
            Err(err) => return Err((snapshot, QueryError::ExecutableCompilation { typedb_source: err })),
        };

        // 4: Executor
        Ok(Pipeline::build_write_pipeline(
            snapshot,
            &variable_registry,
            thing_manager,
            executable_stages,
            executable_fetch,
            Arc::new(value_parameters),
        ))
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
