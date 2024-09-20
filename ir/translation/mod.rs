/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::program::block::{BlockContext, ParameterRegistry, VariableRegistry};

mod constraints;
mod expression;
pub mod function;
pub mod literal;
pub mod match_;
pub mod modifiers;
pub mod tokens;
pub mod writes;

#[derive(Debug)]
pub struct TranslationContext {
    pub variable_registry: VariableRegistry, // TODO: Unpub
    pub visible_variables: HashMap<String, Variable>,
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

    pub fn next_block_context(&mut self) -> BlockContext<'_> {
        let Self { variable_registry, visible_variables, parameters } = self;
        BlockContext::new(variable_registry, visible_variables, parameters)
    }
}
