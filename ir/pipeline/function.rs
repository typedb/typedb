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
use typeql::schema::definable::function::SingleSelector;

use crate::{
    pattern::Vertex,
    pipeline::reduce::Reducer,
    translation::{pipeline::TranslatedStage, TranslationContext},
};

pub type PlaceholderTypeQLReturnOperation = String;

#[derive(Debug, Clone)]
pub struct Function {
    context: TranslationContext,
    name: String,
    function_body: FunctionBody,
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
}

impl Function {
    pub fn new(name: &str, context: TranslationContext, arguments: Vec<Variable>, function_body: FunctionBody) -> Self {
        Self { name: name.to_string(), context, function_body, arguments }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arguments(&self) -> &[Variable] {
        &self.arguments
    }

    pub fn translation_context(&self) -> &TranslationContext {
        &self.context
    }

    pub fn body(&self) -> &FunctionBody {
        &self.function_body
    }
}

#[derive(Debug, Clone)]
pub struct AnonymousFunction {
    translation_context: TranslationContext,
    body: FunctionBody,
}

impl AnonymousFunction {
    pub(crate) fn new(context: TranslationContext, body: FunctionBody) -> Self {
        Self { translation_context: context, body }
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
    Single(SingleSelector, Vec<Variable>),
    ReduceCheck(),
    ReduceReducer(Vec<Reducer>),
}

impl ReturnOperation {
    pub fn return_types(
        &self,
        function_variable_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        match self {
            ReturnOperation::Stream(vars) => {
                let inputs = vars.iter().map(|&var| function_variable_annotations.get(&Vertex::Variable(var)).unwrap());
                inputs.map(|types| BTreeSet::from_iter(types.iter().cloned())).collect()
            }
            ReturnOperation::Single(_, vars) => {
                let inputs = vars.iter().map(|&var| function_variable_annotations.get(&Vertex::Variable(var)).unwrap());
                inputs.map(|types| BTreeSet::from_iter(types.iter().cloned())).collect()
            }
            ReturnOperation::ReduceReducer(reducers) => {
                // aggregates return value types?
                todo!()
            }
            ReturnOperation::ReduceCheck() => {
                // aggregates return value types?
                todo!()
            }
        }
    }
}

impl ReturnOperation {
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::ReduceReducer(_) | Self::Single(_, _) | Self::ReduceCheck() => false,
        }
    }
}
