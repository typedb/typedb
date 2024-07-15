/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
};

use itertools::Itertools;

use crate::{pattern::IrID, program::function_signature::FunctionID};

#[derive(Debug, Clone)]
pub struct FunctionCall<ID: IrID> {
    function_id: FunctionID,
    // map call variable to index of argument
    call_variable_mapping: BTreeMap<ID, usize>,
    return_is_stream: bool, // TODO: This is only used in display. Is that reason to keep it?
}

impl<ID: IrID> FunctionCall<ID> {
    pub fn new(function_id: FunctionID, call_variable_mapping: BTreeMap<ID, usize>, return_is_stream: bool) -> Self {
        Self { function_id, call_variable_mapping, return_is_stream }
    }

    pub(crate) fn function_id(&self) -> FunctionID {
        self.function_id.clone()
    }

    pub(crate) fn call_id_mapping(&self) -> &BTreeMap<ID, usize> {
        &self.call_variable_mapping
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

        write!(f, "fn_{}({})", self.function_id, formatted_args)
    }
}
