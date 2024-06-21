/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use encoding::graph::definition::definition_key::DefinitionKey;
use crate::executor::function_executor::FunctionExecutor;
use crate::executor::pattern_executor::PatternExecutor;
use crate::planner::program_plan::ProgramPlan;

pub struct ProgramExecutor {
    entry: PatternExecutor,
    functions: HashMap<DefinitionKey<'static>, FunctionExecutor>
}

impl ProgramExecutor {

    fn new(program_plan: ProgramPlan) -> Self {

        let ProgramPlan { entry: entry_plan, functions: function_plans } = program_plan;
        let entry = PatternExecutor::new(entry_plan);

        // TODO: functions

        Self {
            entry: entry,
            functions: HashMap::new(),
        }
    }
}
