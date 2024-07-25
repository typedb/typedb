/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::value::value::Value;

use crate::program::{block::FunctionalBlock, function_signature::FunctionSignature};

pub type PlaceholderTypeQLReturnOperation = String;

pub struct FunctionIR {
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    return_operation: ReturnOperationIR,
}

impl FunctionIR {
    pub fn new<'a>(block: FunctionalBlock, arguments: Vec<Variable>, return_operation: ReturnOperationIR) -> Self {
        Self { block, arguments, return_operation }
    }

    pub fn arguments(&self) -> &Vec<Variable> {
        &self.arguments
    }
    pub fn block(&self) -> &FunctionalBlock {
        &self.block
    }
    pub fn return_operation(&self) -> &ReturnOperationIR {
        &self.return_operation
    }
}

pub enum ReturnOperationIR {
    Stream(Vec<Variable>),
    Single(Vec<Reducer>),
}

impl ReturnOperationIR {
    pub(crate) fn output_annotations(
        &self,
        function_variable_annotations: &HashMap<Variable, Arc<HashSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        match self {
            ReturnOperationIR::Stream(vars) => {
                let inputs = vars.iter().map(|v| function_variable_annotations.get(v).unwrap());
                inputs
                    .map(|types_as_arced_hashset| BTreeSet::from_iter(types_as_arced_hashset.iter().map(|t| t.clone())))
                    .collect()
            }
            ReturnOperationIR::Single(_) => {
                todo!()
            }
        }
    }
}

impl ReturnOperationIR {
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::Single(_) => false,
        }
    }
}

pub enum Reducer {
    Count(ReducerInput),
    Sum(ReducerInput),
    // First, Any etc.
}

pub enum ReducerInput {
    Variable,
    Reducer,
}

// TODO: Initialise & use
pub type BuiltInFunctionPtr = fn(Vec<&Value<'static>>) -> Value<'static>;

pub struct BuiltInFunctions {
    index: HashMap<String, usize>,
    signatures: Vec<FunctionSignature>,
    ptrs: Vec<BuiltInFunctionPtr>,
}

impl BuiltInFunctions {
    pub fn new(
        index: HashMap<String, usize>,
        signatures: Vec<FunctionSignature>,
        ptrs: Vec<BuiltInFunctionPtr>,
    ) -> Self {
        Self { index, signatures, ptrs }
    }

    pub fn get_function_key(&self, name: &str) -> Option<usize> {
        self.index.get(name).map(|id| *id)
    }

    pub fn get_function_signature(&self, id: usize) -> Option<&FunctionSignature> {
        self.signatures.get(id)
    }

    pub fn get_function(&self, id: usize) -> Option<&BuiltInFunctionPtr> {
        self.ptrs.get(id)
    }
}
