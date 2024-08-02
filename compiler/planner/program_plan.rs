/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    inference::type_annotations::TypeAnnotations,
    planner::{function_plan::FunctionPlan, pattern_plan::PatternPlan},
};

pub struct ProgramPlan {
    pub entry: PatternPlan,
    pub entry_annotations: TypeAnnotations,
    pub functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
}

impl ProgramPlan {
    pub fn new(
        entry: PatternPlan,
        entry_annotations: TypeAnnotations,
        functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
    ) -> Self {
        Self { entry, entry_annotations, functions }
    }


}
