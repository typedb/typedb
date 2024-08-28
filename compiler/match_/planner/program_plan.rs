/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    expression::compiled_expression::{CompiledExpression, ExpressionValueType},
    match_::planner::{function_plan::FunctionPlan, pattern_plan::MatchProgram},
};

pub struct ProgramPlan {
    // TODO: Update 'Program' to refer to the whole pipeline & to
    // TODO: krishnan: Revert pub
    pub entry: MatchProgram,
    pub entry_value_type_annotations: HashMap<Variable, ExpressionValueType>,
    pub functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
}

impl ProgramPlan {
    pub fn new(
        entry_plan: MatchProgram,
        entry_expressions: HashMap<Variable, CompiledExpression>,
        functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
    ) -> Self {
        let entry_value_type_annotations =
            entry_expressions.iter().map(|(variable, expression)| (*variable, expression.return_type())).collect();
        Self { entry: entry_plan, entry_value_type_annotations, functions }
    }

    pub fn entry(&self) -> &MatchProgram {
        &self.entry
    }

    pub fn entry_value_type_annotations(&self) -> &HashMap<Variable, ExpressionValueType> {
        &self.entry_value_type_annotations
    }
}
