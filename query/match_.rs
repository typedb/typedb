/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::sync::Arc;
use compiler::inference::type_inference::infer_types;
use compiler::planner::pattern_plan::PatternPlan;
use compiler::planner::program_plan::ProgramPlan;
use concept::error::ConceptReadError;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use executor::batch::ImmutableRow;
use executor::program_executor::ProgramExecutor;
use function::function_manager::FunctionManager;
use ir::program::function::Function;
use ir::program::function_signature::FunctionSignatureIndex;
use ir::program::program::Program;
use ir::translation::match_::translate_match;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;
use crate::error::QueryError;

pub(crate) struct MatchClause {
    program_plan: ProgramPlan,
}

impl MatchClause {
    pub(crate) fn new(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        statistics: &Statistics,
        function_index: &impl FunctionSignatureIndex,
        functions: &Vec<Function>,
        match_: &typeql::query::stage::Match,
    ) -> Result<Self, QueryError> {
        let entry = translate_match(function_index, match_)
            .map_err(|err| QueryError::Pattern { source: err })?
            .finish();
        let program = Program::new(entry, functions.clone());

        let annotated_schema_functions = function_manager.get_annotated_functions(snapshot, type_manager)
            .map_err(|err| QueryError::Function { source: err })?;
        let annotated_program = infer_types(program, &snapshot, &type_manager, annotated_schema_functions)
            .map_err(|err| QueryError::MatchWithFunctionsTypeInferenceFailure { clause: match_.clone(), source: err });
        let pattern_plan = PatternPlan::from_block(
            annotated_program.get_entry(), annotated_program.get_entry_annotations(), &statistics
        );
        // TODO: annotate and plan functions
        debug_assert!(!annotated_program.contains_functions(), "Function planning and execution is not implemented.");
        let program_plan = ProgramPlan::new(pattern_plan, annotated_program.get_entry_annotations().clone(), HashMap::new());

        Self { program_plan }
    }

    pub(crate) fn execute(
        &self,
        snapshot: Arc<impl ReadableSnapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
    ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        let executor = ProgramExecutor::new(&self.program_plan, snapshot.as_ref(), &thing_manager)
            .map_err(|err| QueryError::ReadError { source: err })?;
        Ok(executor.into_iterator(snapshot, thing_manager))
    }
}
