/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::program::{block::FunctionalBlock, function::FunctionIR};

pub struct Program {
    pub entry: FunctionalBlock,
    functions: HashMap<DefinitionKey<'static>, FunctionIR>,
}

impl Program {
    pub fn new(entry: FunctionalBlock, functions: HashMap<DefinitionKey<'static>, FunctionIR>) -> Self {
        // TODO: verify exactly the required functions are provided
        debug_assert!(Self::all_variables_categorised(&entry));
        Self { entry, functions }
    }

    pub fn entry(&self) -> &FunctionalBlock {
        &self.entry
    }

    pub fn entry_mut(&mut self) -> &mut FunctionalBlock {
        &mut self.entry
    }

    pub fn compile(match_: &typeql::query::stage::Match) -> Self {
        let _entry = FunctionalBlock::from_match(match_);
        todo!()
    }

    fn all_variables_categorised(block: &FunctionalBlock) -> bool {
        let context = block.context();
        let mut variables = context.variables();
        variables.all(|var| context.get_variable_category(var).is_some())
    }

    pub(crate) fn functions(&self) -> &HashMap<DefinitionKey<'static>, FunctionIR> {
        &self.functions
    }
}
