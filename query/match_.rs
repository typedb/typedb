/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use answer::Type;
use answer::variable::Variable;
use compiler::compiler::compile;
use compiler::inference::type_inference::infer_types;
use compiler::planner::pattern_plan::PatternPlan;
use compiler::planner::program_plan::ProgramPlan;
use concept::error::ConceptReadError;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::{ValueType, ValueTypeCategory};
use executor::batch::ImmutableRow;
use executor::program_executor::ProgramExecutor;
use function::function_manager::FunctionManager;
use ir::pattern::variable_category::{VariableCategory, VariableOptionality};
use ir::program::function::Function;
use ir::program::function_signature::FunctionSignatureIndex;
use ir::program::program::Program;
use ir::translation::match_::translate_match;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;
use crate::error::QueryError;
use crate::pipeline::{NonTerminalStage, NonTerminalStageAPI};

pub(crate) struct MatchClause {
    program_plan: ProgramPlan,
    return_descriptor: HashMap<Variable, (VariableCategory, VariableOptionality)>,
}

// TODO: we can include a sequence of operators into the Match clause:
//       (Select, Limit, Sort).
//       Select | Limit | Sort (this may not be actually included depending on the query plan!)
//
impl MatchClause {
    pub(crate) fn new(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        function_index: &impl FunctionSignatureIndex,
        statistics: &Statistics,
        functions: &Vec<Function>,
        match_: &typeql::query::stage::Match,

        // TODO: previous stage's variable -> name mapping, so we can re-use the same variable ID for the same names
        // TODO: pass in previous stage's return descriptor for compatibility check... should we include inferred types as well? Yes!
        predecessor: &NonTerminalStage,
    ) -> Result<Self, QueryError> {

        // TODO: when we create the IR a stage, we need to first check if the previous stage is returning the named variable for each named variable (first pass)
        //       that we are about to register into the context. If yes, we should register the same variable again (ie. same ID).
        //       inside the context, we can bump the allocator to be (just re-used) + 1 to make sure we never accidentally reuse IDs that pre-allocated

        let entry = translate_match(function_index, match_)
            .map_err(|err| QueryError::Pattern { source: err })?
            .finish();
        let program = Program::new(entry, functions.clone());
        let schema_functions = function_manager.get_annotated_functions(snapshot, type_manager)
            .map_err(|err| QueryError::Function { source: err })?;

        // TODO: compilation needs to take into the previous stage's type annotations and value type annotations
        let program_plan = compile(snapshot, type_manager, schema_functions, statistics, program);

        let mut return_descriptor = HashMap::new();
        for variable in program_plan.entry().outputs() {
            debug_assert!(program_plan.entry().context().get_variables_named().contains_key(variable));
            let category = program_plan.entry().context().get_variable_category(*variable).unwrap();
            let optionality = program_plan.entry().context().get_variable_optionality(*variable).unwrap();
            return_descriptor.insert(*variable, (category, optionality));
        }

        Ok(Self { program_plan, return_descriptor })
    }

    pub(crate) fn execute<'b, Snapshot: ReadableSnapshot + 'static>(
        &self,
        snapshot: Arc<Snapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
    ) -> Result<impl for<'a> LendingIterator<Item<'a>=Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        let executor = ProgramExecutor::new(&self.program_plan, snapshot.as_ref(), &thing_manager)
            .map_err(|err| QueryError::ReadError { source: err })?;
        Ok(executor.into_iterator(snapshot, thing_manager))
    }
}

impl NonTerminalStageAPI for MatchClause {
    fn return_descriptor(&self) -> &HashMap<Variable, (VariableCategory, VariableOptionality)> {
        &self.return_descriptor
    }

    fn variable_type_annotations(&self, variable: Variable) -> Option<&HashSet<Type>> {
        self.program_plan.entry_type_annotations().variable_annotations_of(variable).map(|arc| arc.as_ref())
    }

    fn variable_value_type(&self, variable: Variable) -> Option<ValueTypeCategory> {
        self.program_plan.entry_value_type_annotations().get(&variable).cloned()
    }

    fn get_named_return_variable(&self, name: &str) -> Option<Variable> {
        self.program_plan.entry().context().named_variable_mapping().get(name)
            .cloned()
            .filter(|variable| self.return_descriptor.contains_key(&variable))
    }
}
