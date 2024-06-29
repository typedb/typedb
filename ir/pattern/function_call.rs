/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    hash::Hash,
};
use std::collections::BTreeMap;
use std::hash::Hasher;

use answer::variable::Variable;
use encoding::graph::definition::definition_key::DefinitionKey;
use itertools::Itertools;

use crate::pattern::{
    variable_category::{VariableCategory, VariableOptionality},
    IrID,
};

/// This IR has information copied from the target function, so inference can be block-local
#[derive(Debug, Clone)]
pub struct FunctionCall<ID: IrID> {
    function_id: DefinitionKey<'static>,
    // map call variable to function-internal varirable
    call_variable_mapping: BTreeMap<ID, Variable>,
    // map call variable to category of variable as indicated by function signature
    call_variable_categories: HashMap<ID, VariableCategory>,
    returns: Vec<(VariableCategory, VariableOptionality)>,
    return_is_stream: bool,
}

impl<ID: IrID> FunctionCall<ID> {
    pub fn new(
        function_id: DefinitionKey<'static>,
        call_variable_mapping: BTreeMap<ID, Variable>,
        call_variable_categories: HashMap<ID, VariableCategory>,
        returns: Vec<(VariableCategory, VariableOptionality)>,
        return_is_stream: bool,
    ) -> Self {
        Self { function_id, call_variable_mapping, call_variable_categories, returns, return_is_stream }
    }

    pub(crate) fn call_id_mapping(&self) -> &BTreeMap<ID, Variable> {
        &self.call_variable_mapping
    }

    pub(crate) fn returns(&self) -> &Vec<(VariableCategory, VariableOptionality)> {
        &self.returns
    }

    pub(crate) fn return_is_stream(&self) -> bool {
        self.return_is_stream
    }
}

impl<ID: IrID> PartialEq for FunctionCall<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.function_id == other.function_id && self.call_variable_mapping == other.call_variable_mapping
    }
}

impl<ID: IrID> Eq for FunctionCall<ID> {}

impl<ID: IrID> Hash for FunctionCall<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function_id.hash(state);
        self.call_variable_mapping.hash(state);
    }
}

impl<ID: IrID> Display for FunctionCall<ID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let formatted_args = self
            .call_variable_mapping
            .iter()
            .map(|(call_var, function_var)| format!("{} = {}", function_var, call_var))
            .join(", ");

        let formatted_is_stream = if self.return_is_stream { "Stream" } else { "Single" };
        let formatted_return = self
            .returns
            .iter()
            .map(|(category, optionality)| match optionality {
                VariableOptionality::Required => format!("{}", category),
                VariableOptionality::Optional => format!("{}?", category),
            })
            .join(", ");

        write!(f, "fn_{}({}) -> {}({})", self.function_id, formatted_args, formatted_is_stream, formatted_return)
    }
}
