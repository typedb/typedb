/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;
use answer::variable::Variable;
use typeql::{schema::definable::function::SingleSelector, TypeRefAny};

use crate::{
    pipeline::reduce::Reducer,
    translation::{pipeline::TranslatedStage, TranslationContext},
};

#[derive(Debug, Clone)]
pub struct Function {
    pub context: TranslationContext,
    pub name: String,
    pub function_body: FunctionBody,
    // Variable categories for args & return can be read from the block's context.
    pub arguments: Vec<Variable>,
    pub argument_labels: Option<Vec<TypeRefAny>>,
}

impl Function {
    pub fn new(
        name: &str,
        context: TranslationContext,
        arguments: Vec<Variable>,
        argument_labels: Option<Vec<TypeRefAny>>,
        function_body: FunctionBody,
    ) -> Self {
        Self { name: name.to_string(), context, function_body, arguments, argument_labels }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn translation_context(&self) -> &TranslationContext {
        &self.context
    }

    pub fn body(&self) -> &FunctionBody {
        &self.function_body
    }
}

#[derive(Debug, Clone)]
pub struct FunctionBody {
    pub stages: Vec<TranslatedStage>,
    pub return_operation: ReturnOperation,
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
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::ReduceReducer(_) | Self::Single(_, _) | Self::ReduceCheck() => false,
        }
    }

    pub(crate) fn variables<'a>(&'a self) -> Cow<'a, [Variable]> {
        match self {
            ReturnOperation::Stream(vars) => Cow::Borrowed(vars),
            ReturnOperation::Single(_, vars) => Cow::Borrowed(vars),
            ReturnOperation::ReduceCheck() => Cow::Owned(vec![]),
            ReturnOperation::ReduceReducer(reducers) => {
                let vars: Vec<_> = reducers.iter().map(|reducer| reducer.variable()).flatten().collect();
                Cow::Owned(vars)
            }
        }
    }
}
