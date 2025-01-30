/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use typeql::common::Span;

use crate::{
    pattern::variable_category::VariableCategory,
    pipeline::{block::BlockBuilderContext, reduce::Reducer, ParameterRegistry, VariableRegistry},
};

mod constraints;
mod expression;
pub mod fetch;
pub mod function;
pub mod literal;
pub mod match_;
pub mod modifiers;
pub mod pipeline;
pub mod reduce;
pub mod tokens;
pub mod writes;

#[derive(Debug, Clone)]
pub struct TranslationContext {
    pub variable_registry: VariableRegistry, // TODO: Unpub
    visible_variables: HashMap<String, Variable>,
}

impl Default for TranslationContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TranslationContext {
    pub fn new() -> Self {
        Self { variable_registry: VariableRegistry::new(), visible_variables: HashMap::new() }
    }

    pub fn new_with_function_arguments(
        input_variables: Vec<(String, Option<Span>, VariableCategory)>,
    ) -> (Self, Vec<Variable>) {
        let mut visible_variables = HashMap::new();
        let mut variable_registry = VariableRegistry::new();
        let mut variables = Vec::with_capacity(input_variables.len());
        for (name, source_span, category) in input_variables {
            let variable = variable_registry.register_function_argument(name.as_str(), category, source_span);
            visible_variables.insert(name.clone(), variable);
            variables.push(variable);
        }
        let this = Self { variable_registry, visible_variables };
        (this, variables)
    }

    pub fn new_block_builder_context<'a>(
        &'a mut self,
        parameters: &'a mut ParameterRegistry,
    ) -> BlockBuilderContext<'a> {
        let Self { variable_registry, visible_variables } = self;
        BlockBuilderContext::new(variable_registry, visible_variables, parameters)
    }

    pub(crate) fn register_reduced_variable(
        &mut self,
        name: &str,
        variable_category: VariableCategory,
        is_optional: bool,
        source_span: Option<Span>,
        reducer: Reducer,
    ) -> Variable {
        let variable = self.variable_registry.register_reduce_output_variable(
            name.to_owned(),
            variable_category,
            is_optional,
            source_span,
            reducer,
        );
        self.visible_variables.insert(name.to_owned(), variable);
        variable
    }

    pub fn get_variable(&self, variable: &str) -> Option<Variable> {
        self.visible_variables.get(variable).cloned()
    }
}
