/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};

use crate::{
    pattern::Vertex,
    program::block::{FunctionalBlock, VariableRegistry},
};
use crate::pattern::IrID;

pub type PlaceholderTypeQLReturnOperation = String;

#[derive(Debug, Clone)]
pub struct Function {
    name: String,
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    variable_registry: VariableRegistry,
    return_operation: ReturnOperation,
}

impl Function {
    pub fn new(
        name: &str,
        block: FunctionalBlock,
        variable_registry: VariableRegistry,
        arguments: Vec<Variable>,
        return_operation: ReturnOperation,
    ) -> Self {
        Self { name: name.to_string(), block, variable_registry, arguments, return_operation }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arguments(&self) -> &[Variable] {
        &self.arguments
    }

    pub fn block(&self) -> &FunctionalBlock {
        &self.block
    }

    pub fn variable_registry(&self) -> &VariableRegistry {
        &self.variable_registry
    }

    pub fn return_operation(&self) -> &ReturnOperation {
        &self.return_operation
    }
}

#[derive(Debug, Clone)]
pub enum ReturnOperation {
    Stream(Vec<Variable>),
    Reduce(Vec<Reducer<Variable>>),
}

impl ReturnOperation {
    pub fn output_annotations(
        &self,
        function_variable_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        match self {
            ReturnOperation::Stream(vars) => {
                let inputs = vars.iter().map(|&var| function_variable_annotations.get(&Vertex::Variable(var)).unwrap());
                inputs
                    .map(|types_as_arced_hashset| BTreeSet::from_iter(types_as_arced_hashset.iter().cloned()))
                    .collect()
            }
            ReturnOperation::Reduce(_) => {
                todo!()
            }
        }
    }
}

impl ReturnOperation {
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::Reduce(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Reducer<ID: IrID> {
    Count(ID),
    SumLong(ID),
    // First, Any etc.
}

impl<ID: IrID> Reducer<ID> {
    pub fn id(&self) -> ID {
        match self {
            Reducer::Count(id) => id.clone(),
            Reducer::SumLong(id) => id.clone(),
        }
    }
}
