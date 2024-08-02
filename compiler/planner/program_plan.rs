/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::variable::Variable;
use concept::thing::statistics::Statistics;

use encoding::graph::definition::definition_key::DefinitionKey;
use encoding::value::value_type::{ValueType, ValueTypeCategory};

use crate::{
    inference::type_annotations::TypeAnnotations,
    planner::{function_plan::FunctionPlan, pattern_plan::PatternPlan},
};
use crate::expression::compiled_expression::CompiledExpression;
use crate::inference::annotated_program::AnnotatedProgram;

pub struct ProgramPlan {
    entry: PatternPlan,
    entry_type_annotations: TypeAnnotations,
    // TODO: this should have ValueType not ValueTypeCategory
    entry_value_type_annotations: HashMap<Variable, ValueTypeCategory>,
    functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
}

impl ProgramPlan {
    pub fn new(
        entry_plan: PatternPlan,
        entry_annotations: TypeAnnotations,
        entry_expressions: HashMap<Variable, CompiledExpression>,
        functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
    ) -> Self {
        let mut entry_value_type_annotations = HashMap::new();
        for (variable, expression) in &entry_expressions {
            entry_value_type_annotations.insert(*variable, expression.return_type().as_variable_category())
        }

        Self {
            entry: entry_plan,
            entry_type_annotations: entry_annotations,
            entry_value_type_annotations: entry_value_type_annotations,
            functions,
        }
    }

    pub fn from_program(
        program: AnnotatedProgram,
        statistics: &Statistics,
    ) -> Self {
        let AnnotatedProgram { entry, entry_annotations, entry_expressions, schema_functions, preamble_functions } = program;
        let entry_plan = PatternPlan::from_block(
            &entry, &entry_annotations, &entry_expressions, &statistics,
        );
        // TODO: plan all premable functions and merge with schema functions
        Self::new(entry_plan, entry_annotations, entry_expressions, HashMap::new())
    }

    pub fn entry(&self) -> &PatternPlan {
        &self.entry
    }

    pub fn entry_type_annotations(&self) -> &TypeAnnotations {
        &self.entry_type_annotations
    }

    pub fn entry_value_type_annotations(&self) -> &HashMap<Variable, ValueTypeCategory> {
        &self.entry_value_type_annotations
    }
}
