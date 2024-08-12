/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{thing::statistics::Statistics, type_::type_manager::TypeManager};
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    program::{
        function_signature::{FunctionID, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        FunctionDefinitionError,
    },
    translation::function::translate_function,
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::{Pipeline, SchemaQuery};

use crate::{define, error::QueryError};
// use crate::match_::MatchClause;
//
// use crate::pipeline::NonTerminalStage as ExecutableStage; // TODO: Type didn't exist otherwise
// use crate::pipeline::ExecutableStage;

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
        query: SchemaQuery,
    ) -> Result<(), QueryError> {
        // let parsed = typeql::parse_query(query)
        //     .map_err(|err| QueryError::ParseError { typeql_query: query.to_string(), source: err })?;
        match query {
            SchemaQuery::Define(define) => {
                define::execute(snapshot, &type_manager, define).map_err(|err| QueryError::Define { source: err })
            }
            SchemaQuery::Redefine(redefine) => {
                todo!()
            }
            SchemaQuery::Undefine(undefine) => {
                todo!()
            }
        }
    }
    //
    // pub fn prepare_pipeline(
    //     &self,
    //     snapshot: &impl ReadableSnapshot,
    //     type_manager: &TypeManager,
    //     function_manager: &FunctionManager,
    //     function_index: &impl FunctionSignatureIndex,
    //     statistics: &Statistics,
    //     pipeline: Pipeline,
    // ) -> Result<Vec<ExecutableStage>, QueryError> {
    //     let query_functions_signatures = HashMapFunctionSignatureIndex::build(
    //         pipeline.preambles.iter()
    //             .enumerate()
    //             .map(|(index, preamble)| (FunctionID::Preamble(index), &preamble.function))
    //     );
    //     let function_signatures = ReadThroughFunctionSignatureIndex::new(
    //         snapshot, function_manager, query_functions_signatures,
    //     );
    //     let query_functions: Vec<_> = pipeline.preambles.iter().map(|preamble|
    //         translate_function(&function_signatures, &preamble.function)
    //     )
    //         .collect::<Result<Vec<_>, FunctionDefinitionError>>()
    //         .map_err(|err| QueryError::PipelineFunctionDefinition { source: err })?;
    //
    //     let mut executable_pipeline = Vec::new();
    //     for stage in &pipeline.stages {
    //         let executable_stage = match stage {
    //             typeql::query::stage::Stage::Match(match_) => ExecutableStage::Match(MatchClause::new(
    //                 snapshot,
    //                 type_manager,
    //                 function_manager,
    //                 function_index,
    //                 statistics,
    //                 &query_functions,
    //                 match_,
    //                 executable_pipeline.last()
    //             )?),
    //             typeql::query::stage::Stage::Insert(insert) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Put(put) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Update(update) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Fetch(fetch) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Delete(delete) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Reduce(reduce) => {
    //                 todo!()
    //             }
    //             typeql::query::stage::Stage::Modifier(modifier) => {
    //                 todo!()
    //             }
    //         };
    //         executable_pipeline.push(executable_stage);
    //     };
    //     Ok(executable_pipeline)
    // }
}
