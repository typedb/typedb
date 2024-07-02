/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;

use crate::{
    PatternDefinitionError,
    program::{
        FunctionalBlock,
        modifier::Modifier,
    },
};
use crate::pattern::conjunction::Conjunction;

pub struct FunctionIR {
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    // TODO: how to encode return operation?
}

impl FunctionIR {
    fn new<'a>(
        block: FunctionalBlock,
        arguments: impl Iterator<Item=&'a str>,
    ) -> Result<Self, PatternDefinitionError> {
        let mut argument_variables = Vec::new();
        {
            let context = block.context();
            for arg in arguments {
                let var = context.get_variable(arg).ok_or_else(|| PatternDefinitionError::FunctionArgumentUnused {
                    argument_variable: arg.to_string(),
                })?;
                argument_variables.push(var);
            }
        }
        Ok(Self { arguments: argument_variables, block })
    }
}
