/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    hash::{DefaultHasher, Hasher},
    mem,
};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::{
    common::Span,
    schema::definable::function::{Output, SingleSelector},
    type_::NamedTypeAny,
};

use crate::{
    pipeline::{reduce::Reducer, ParameterRegistry},
    translation::{pipeline::TranslatedStage, TranslationContext},
};

#[derive(Debug, Clone)]
pub struct Function {
    pub context: TranslationContext,
    pub parameters: ParameterRegistry,
    pub name: String,
    pub function_body: FunctionBody,
    pub output: Option<Output>,
    // Variable categories for args & return can be read from the block's context.
    pub arguments: Vec<Variable>,
    pub argument_labels: Option<Vec<NamedTypeAny>>,
}

impl Function {
    pub fn new(
        name: &str,
        context: TranslationContext,
        parameters: ParameterRegistry,
        arguments: Vec<Variable>,
        argument_labels: Option<Vec<NamedTypeAny>>,
        output: Option<Output>,
        function_body: FunctionBody,
    ) -> Self {
        Self { name: name.to_string(), context, parameters, function_body, output, arguments, argument_labels }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn translation_context(&self) -> &TranslationContext {
        &self.context
    }

    pub fn parameters(&self) -> &ParameterRegistry {
        &self.parameters
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
    Stream(Vec<Variable>, Option<Span>),
    Single(SingleSelector, Vec<Variable>, Option<Span>),
    ReduceCheck(Option<Span>),
    ReduceReducer(Vec<Reducer>, Option<Span>),
}

impl ReturnOperation {
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_, _) => true,
            Self::ReduceReducer(_, _) | Self::Single(_, _, _) | Self::ReduceCheck(_) => false,
        }
    }

    pub(crate) fn is_scalar(&self) -> bool {
        match self {
            Self::Stream(vars, _) => vars.len() < 2,
            Self::Single(_, vars, _) => vars.len() < 2,
            Self::ReduceCheck(_) => true,
            Self::ReduceReducer(reducers, _) => reducers.len() < 2,
        }
    }

    pub(crate) fn variables(&self) -> Cow<'_, [Variable]> {
        match self {
            ReturnOperation::Stream(vars, _) => Cow::Borrowed(vars),
            ReturnOperation::Single(_, vars, _) => Cow::Borrowed(vars),
            ReturnOperation::ReduceCheck(_) => Cow::Owned(vec![]),
            ReturnOperation::ReduceReducer(reducers, _) => {
                let vars = reducers.iter().filter_map(Reducer::variable).collect();
                Cow::Owned(vars)
            }
        }
    }

    pub fn source_span(&self) -> Option<Span> {
        match self {
            ReturnOperation::Stream(_, source_span)
            | ReturnOperation::Single(_, _, source_span)
            | ReturnOperation::ReduceCheck(source_span)
            | ReturnOperation::ReduceReducer(_, source_span) => *source_span,
        }
    }
}

impl StructuralEquality for Function {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.function_body.hash_into(&mut hasher);
        self.arguments.len().hash_into(&mut hasher);
        self.argument_labels.hash_into(&mut hasher);
        self.output.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.function_body.equals(&other.function_body)
            && self.arguments.len().equals(&other.arguments.len())
            && self.argument_labels.equals(&other.argument_labels)
            && self.output.equals(&other.output)
    }
}

impl StructuralEquality for FunctionBody {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.stages.hash_into(&mut hasher);
        self.return_operation.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.stages().equals(other.stages()) && self.return_operation().equals(other.return_operation())
    }
}

impl StructuralEquality for ReturnOperation {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            ^ match self {
                ReturnOperation::Stream(variables, _) => variables.hash(),
                ReturnOperation::Single(selector, variables, _) => {
                    let mut hasher = DefaultHasher::new();
                    selector.hash_into(&mut hasher);
                    variables.hash_into(&mut hasher);
                    hasher.finish()
                }
                ReturnOperation::ReduceCheck(_) => 0,
                ReturnOperation::ReduceReducer(reducers, _) => {
                    // note: position matters for return operations
                    reducers.hash()
                }
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stream(vars, _), Self::Stream(other_vars, _)) => vars.equals(other_vars),
            (Self::Single(selector, vars, _), Self::Single(other_selector, other_vars, _)) => {
                selector.equals(other_selector) && vars.equals(other_vars)
            }
            (Self::ReduceCheck(_), Self::ReduceCheck(_)) => true,
            (Self::ReduceReducer(inner, _), Self::ReduceReducer(other_inner, _)) => inner.equals(other_inner),
            // note: this style forces updating the match when the variants change
            (Self::Stream { .. }, _)
            | (Self::Single { .. }, _)
            | (Self::ReduceCheck { .. }, _)
            | (Self::ReduceReducer { .. }, _) => false,
        }
    }
}
