/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::program::block::FunctionalBlock;

use crate::program::{
    function::FunctionIR,
};

pub struct Program {
    entry: FunctionalBlock,
    functions: HashMap<DefinitionKey<'static>, FunctionIR>,
}

impl Program {
    pub fn new(entry_block: FunctionalBlock, functions: HashMap<DefinitionKey<'static>, FunctionIR>) -> Self {
        // TODO: verify exactly the required functions are provided
        debug_assert!(Self::all_variables_categorised(&entry_block));
        Self { entry: entry_block, functions: functions }
    }

    pub fn entry(&mut self) -> &mut FunctionalBlock {
        &mut self.entry
    }

    fn all_variables_categorised(block: &FunctionalBlock) -> bool {
        let context = block.context();
        let mut variables = context.get_variables();
        variables.all(|var| context.get_variable_category(var).is_some())
    }
}
