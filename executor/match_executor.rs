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
use storage::snapshot::ReadableSnapshot;

use crate::{
    function_executor::FunctionExecutor,
    pattern_executor::{PatternExecutor, PatternIterator},
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    ExecutionInterrupt, VariablePosition,
};

pub struct MatchExecutor {
    entry: PatternExecutor,
    functions: HashMap<DefinitionKey<'static>, FunctionExecutor>,
}

impl MatchExecutor {
    pub fn new(
        program_plan: &ProgramPlan,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        input: MaybeOwnedRow<'_>,
    ) -> Result<Self, ConceptReadError> {
        let ProgramPlan { entry: entry_plan, functions: function_plans, entry_value_type_annotations } = program_plan;
        let entry = PatternExecutor::new(entry_plan, snapshot, thing_manager, input)?;

        // TODO: functions

        Ok(Self { entry, functions: HashMap::new() })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> PatternIterator<Snapshot> {
        self.entry.into_iterator(context, interrupt)
    }
}
