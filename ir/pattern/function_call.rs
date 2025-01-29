/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
};

use itertools::Itertools;
use structural_equality::StructuralEquality;

use crate::{pattern::IrID, pipeline::function_signature::FunctionID};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionCall<ID> {
    function_id: FunctionID,
    // map call variable to index of argument
    call_variable_mapping: BTreeMap<ID, usize>,
}

impl<ID> FunctionCall<ID> {
    pub fn new(function_id: FunctionID, call_variable_mapping: BTreeMap<ID, usize>) -> Self {
        Self { function_id, call_variable_mapping }
    }
}

impl<ID: IrID> FunctionCall<ID> {
    pub fn function_id(&self) -> FunctionID {
        self.function_id.clone()
    }

    pub fn call_id_mapping(&self) -> &BTreeMap<ID, usize> {
        &self.call_variable_mapping
    }

    pub fn argument_ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.call_variable_mapping.keys().cloned()
    }

    pub fn map<T: Clone + Ord>(self, mapping: &HashMap<ID, T>) -> FunctionCall<T> {
        FunctionCall::new(
            self.function_id.clone(),
            self.call_variable_mapping.iter().map(|(k, v)| (k.map(mapping), v.clone())).collect(),
        )
    }
}

impl<ID: StructuralEquality + Ord> StructuralEquality for FunctionCall<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.function_id.hash_into(&mut hasher);
        self.call_variable_mapping.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.function_id.equals(&other.function_id) && self.call_variable_mapping.equals(&other.call_variable_mapping)
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
