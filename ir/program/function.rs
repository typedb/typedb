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
    program::{reduce::Reducer, VariableRegistry},
    translation::pipeline::TranslatedStage,
};

pub type PlaceholderTypeQLReturnOperation = String;

#[derive(Debug, Clone)]
pub struct Function {
    name: String,
    function_body: FunctionBody,
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    variable_registry: VariableRegistry,
}

impl Function {
    pub fn new(
        name: &str,
        variable_registry: VariableRegistry,
        arguments: Vec<Variable>,
        function_body: FunctionBody,
    ) -> Self {
        Self { name: name.to_string(), variable_registry, function_body, arguments }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arguments(&self) -> &[Variable] {
        &self.arguments
    }

    pub fn variable_registry(&self) -> &VariableRegistry {
        &self.variable_registry
    }

    pub fn body(&self) -> &FunctionBody {
        &self.function_body
    }
}

#[derive(Debug, Clone)]
pub struct FunctionBody {
    stages: Vec<TranslatedStage>,
    return_operation: ReturnOperation,
}

impl FunctionBody {
    pub fn new(stages: Vec<TranslatedStage>, return_operation: ReturnOperation) -> Self {
        Self { stages, return_operation }
    }

    pub fn stages(&self) -> &[TranslatedStage] {
        &self.stages
    }

    pub fn return_operation(&self) -> &ReturnOperation {
        &self.return_operation
    }
}

#[derive(Debug, Clone)]
pub enum ReturnOperation {
    Stream(Vec<Variable>),
    Reduce(Vec<Reducer>),
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
