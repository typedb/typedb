/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, fmt, hash::Hash};

use itertools::Itertools;

use crate::{
    pattern::{IrID, Vertex},
    pipeline::function_signature::FunctionID,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionCall<ID> {
    function_id: FunctionID,
    // map call variable to index of argument
    call_variable_mapping: BTreeMap<ID, usize>,
}

impl<ID: IrID> FunctionCall<ID> {
    pub fn new(function_id: FunctionID, call_variable_mapping: BTreeMap<ID, usize>) -> Self {
        Self {
            function_id,
            call_variable_mapping,
        }
    }

    pub fn function_id(&self) -> FunctionID {
        self.function_id.clone()
    }

    pub fn call_id_mapping(&self) -> &BTreeMap<ID, usize> {
        &self.call_variable_mapping
    }

    pub fn argument_ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.call_variable_mapping.keys().cloned()
    }
}

impl<ID: IrID> fmt::Display for FunctionCall<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted_args = self
            .call_variable_mapping
            .iter()
            .map(|(call_var, function_var)| format!("{} = {}", function_var, call_var))
            .join(", ");

        write!(f, "fn_{}({})", self.function_id, formatted_args)
    }
}
