/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::pattern::variable::{Variable, VariableCategory, VariableOptionality};

/// This IR has information copied from the target function, so inference can be block-local
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunctionCall {
    function_id: DefinitionKey<'static>,
    call_variable_mapping: HashMap<Variable, Variable>,
    call_variable_categories: HashMap<Variable, VariableCategory>,
    returns: Vec<(VariableCategory, VariableOptionality)>,
    return_is_stream: bool,
}

impl FunctionCall {
    pub fn new(
        function_id: DefinitionKey<'static>,
        call_variable_mapping: HashMap<Variable, Variable>,
        call_variable_categories: HashMap<Variable, VariableCategory>,
        returns: Vec<(VariableCategory, VariableOptionality)>,
        return_is_stream: bool,
    ) -> Self {
        Self { function_id, call_variable_mapping, call_variable_categories, returns, return_is_stream }
    }

    pub(crate) fn returns(&self) -> &Vec<(VariableCategory, VariableOptionality)> {
        &self.returns
    }
}

impl Display for FunctionCall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let formatted_args = self.call_variable_mapping
            .iter()
            .map(|(call_var, function_var)| format!("{} = {}", function_var, call_var))
            .join(", ");

        let formatted_is_stream = if self.return_is_stream { "Stream" } else { "Single" };
        let formatted_return = self.returns
            .iter()
            .map(|(category, optionality)| {
                match optionality {
                    VariableOptionality::Required => format!("{}", category),
                    VariableOptionality::Optional => format!("{}?", category),
                }
            })
            .join(", ");

        write!(f, "fn_{}({}) -> {}({})", self.function_id, formatted_args, formatted_is_stream, formatted_return)
    }
}
