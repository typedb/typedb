/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use concept::error::ConceptReadError;
use concept::thing::thing_manager::ThingManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::inference::type_inference::TypeAnnotations;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{function_executor::FunctionExecutor, pattern_executor::PatternExecutor},
    planner::program_plan::ProgramPlan,
};

pub struct ProgramExecutor {
    entry: PatternExecutor,
    functions: HashMap<DefinitionKey<'static>, FunctionExecutor>,
}

impl ProgramExecutor {
    fn new<Snapshot: ReadableSnapshot>(
        program_plan: ProgramPlan,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Self, ConceptReadError> {
        let ProgramPlan { entry: entry_plan, functions: function_plans } = program_plan;
        let entry = PatternExecutor::new(entry_plan, type_annotations, snapshot, thing_manager)?;

        // TODO: functions

        Ok(Self { entry: entry, functions: HashMap::new() })
    }
}
