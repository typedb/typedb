/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::match_::{inference::annotated_functions::IndexedAnnotatedFunctions};
use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use executor::{
    pipeline::{InitialStage, PipelineContext, WritablePipelineStage},
    write::insert::{InsertExecutor, InsertStage},
};
use function::{function::Function, function_manager::FunctionManager};
use storage::snapshot::WritableSnapshot;
use typeql::query::SchemaQuery;

use crate::{
    compilation::{compile_pipeline, CompiledPipeline, CompiledStage},
    define,
    error::QueryError,
    translation::{translate_pipeline, TranslatedPipeline},
    type_inference::{infer_types_for_pipeline, AnnotatedPipeline},
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
                .map_err(|err| QueryError::Define { source: err }),
            SchemaQuery::Redefine(redefine) => {
                todo!()
            }
            SchemaQuery::Undefine(undefine) => {
                todo!()
            }
        }
    }

    pub fn execute_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        statistics: &Statistics,
        schema_function_annotations: &IndexedAnnotatedFunctions,
        query: &typeql::query::Pipeline,
    ) -> Result<(), QueryError> {
        todo!()
    }

    pub fn prepare_writable_pipeline<Snapshot: WritableSnapshot>(
        &self,
        snapshot: Snapshot,
        thing_manager: ThingManager,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        statistics: &Statistics,
        schema_function_annotations: &IndexedAnnotatedFunctions,
        query: &typeql::query::Pipeline,
    ) -> Result<WritablePipelineStage<Snapshot>, QueryError> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        let mut snapshot = snapshot;
        // 1: Translate
        let TranslatedPipeline { translated_preamble, translated_stages, variable_registry } =
            translate_pipeline(&snapshot, function_manager, query)?;
        // TODO: Do we optimise here or after type-inference?

        // 2: Annotate
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = infer_types_for_pipeline(
            &mut snapshot,
            type_manager,
            schema_function_annotations,
            &variable_registry,
            translated_preamble,
            translated_stages,
        )?;

        // // 3: Compile
        let CompiledPipeline { compiled_functions, compiled_stages } =
            compile_pipeline(statistics, &variable_registry, annotated_preamble, annotated_stages)?;

        let context = PipelineContext::Owned(snapshot, thing_manager);
        let mut latest_stage = WritablePipelineStage::Initial(InitialStage::new(context));
        for compiled_stage in compiled_stages {
            match compiled_stage {
                CompiledStage::Match(match_plan) => {
                    todo!()
                }
                CompiledStage::Insert(insert_plan) => {
                    let insert_stage = InsertStage::new(Box::new(latest_stage), InsertExecutor::new(insert_plan));
                    latest_stage = WritablePipelineStage::Insert(insert_stage);
                }
                CompiledStage::Delete(delete) => {
                    todo!()
                }
            }
        }
        Ok(latest_stage)
    }

    // TODO: take in parsed TypeQL clause
    fn create_executor(&self, clause: &str) {
        // match clause
    }

    fn create_match_executor(&self, query_functions: Vec<Function<usize>>) {
        // let conjunction = Conjunction::new();
        // ... build conjunction...
    }
}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}
