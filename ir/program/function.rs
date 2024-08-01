/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};

use crate::program::block::FunctionalBlock;

pub type PlaceholderTypeQLReturnOperation = String;

pub struct Function {
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    return_operation: ReturnOperation,
}

impl Function {
    pub fn new<'a>(block: FunctionalBlock, arguments: Vec<Variable>, return_operation: ReturnOperation) -> Self {
        Self { block, arguments, return_operation }
    }

    pub fn arguments(&self) -> &Vec<Variable> {
        &self.arguments
    }
    pub fn block(&self) -> &FunctionalBlock {
        &self.block
    }
    pub fn return_operation(&self) -> &ReturnOperation {
        &self.return_operation
    }
}

pub enum ReturnOperation {
    Stream(Vec<Variable>),
    Single(Vec<Reducer>),
}

impl ReturnOperation {
    pub fn output_annotations(
        &self,
        function_variable_annotations: &HashMap<Variable, Arc<HashSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        match self {
            ReturnOperation::Stream(vars) => {
                let inputs = vars.iter().map(|v| function_variable_annotations.get(v).unwrap());
                inputs
                    .map(|types_as_arced_hashset| BTreeSet::from_iter(types_as_arced_hashset.iter().map(|t| t.clone())))
                    .collect()
            }
            ReturnOperation::Single(_) => {
                todo!()
            }
        }
    }
}

impl ReturnOperation {
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
