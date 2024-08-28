/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use compiler::match_::planner::program_plan::ProgramPlan;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use encoding::graph::definition::definition_key::DefinitionKey;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{Batch, ImmutableRow},
    function_executor::FunctionExecutor,
    pattern_executor::PatternExecutor,
    VariablePosition,
};

pub struct ProgramExecutor {
    entry: PatternExecutor,
    functions: HashMap<DefinitionKey<'static>, FunctionExecutor>,
}

impl ProgramExecutor {
    pub fn new(
        program_plan: &ProgramPlan,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let ProgramPlan { entry: entry_plan, functions: function_plans, entry_value_type_annotations } = program_plan;
        let entry = PatternExecutor::new(entry_plan, snapshot, thing_manager)?;

        // TODO: functions

        Ok(Self { entry, functions: HashMap::new() })
    }

    pub fn entry_variable_positions(&self) -> &HashMap<Variable, VariablePosition> {
        self.entry.variable_positions()
    }

    pub fn into_iterator(
        self,
        snapshot: Arc<impl ReadableSnapshot + 'static>,
        thing_manager: Arc<ThingManager>,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        self.entry.into_iterator(snapshot, thing_manager)
    }
}
