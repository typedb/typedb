/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::{
    pattern::variable_category::VariableCategory,
    program::{block::BlockBuilderContext, reduce::Reducer, ParameterRegistry, VariableRegistry},
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
    pub parameters: ParameterRegistry,
}

impl TranslationContext {
    pub fn new() -> Self {
        Self {
            parameters: ParameterRegistry::new(),
            variable_registry: VariableRegistry::new(),
            visible_variables: HashMap::new(),
        }
    }

    pub fn new_block_builder_context(&mut self) -> BlockBuilderContext<'_> {
        let Self { variable_registry, visible_variables, parameters } = self;
        BlockBuilderContext::new(variable_registry, visible_variables, parameters)
    }

    pub(crate) fn register_reduced_variable(
        &mut self,
        name: &str,
        variable_category: VariableCategory,
        is_optional: bool,
        reducer: Reducer,
    ) -> Variable {
        self.variable_registry.register_reduce_output_variable(name, variable_category, is_optional, reducer)
    }

    pub fn get_variable(&self, variable: &str) -> Option<Variable> {
        self.visible_variables.get(variable).cloned()
    }
}
