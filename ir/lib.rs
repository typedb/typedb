/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]
#![allow(unused_variables)]

use std::{
    error::Error,
    fmt::{self},
};

use answer::variable::Variable;

use crate::{
    pattern::{constraint::Constraint, expression::ExpressionDefinitionError, variable_category::VariableCategory},
    program::FunctionReadError,
};

pub mod expressions;
pub mod inference;
mod optimisation;
pub mod pattern;
pub mod program;
pub mod translator;

#[derive(Debug)]
pub enum PatternDefinitionError {
    DisjointVariableReuse {
        variable_name: String,
    },
    VariableCategoryMismatch {
        variable: Variable,
        variable_name: Option<String>,
        category_1: VariableCategory,
        category_1_source: Constraint<Variable>,
        category_2: VariableCategory,
        category_2_source: Constraint<Variable>,
    },
    FunctionCallReturnArgCountMismatch {
        assigned_var_count: usize,
        function_return_count: usize,
    },
    FunctionCallArgumentCountMismatch {
        expected: usize,
        actual: usize,
    },
    FunctionRequiredArgumentReceivedOptionalVariable {
        variable: Variable,
    },
    UnresolvedFunction {
        function_name: String,
    },
    ExpectedStreamReceivedSingle {
        function_name: String,
    },
    ExpectedSingeReceivedStream {
        function_name: String,
    },
    OptionalVariableForRequiredArgument {
        function_name: String,
        index: usize,
    },
    FunctionRead {
        source: FunctionReadError,
    },
    // TODO: Should expressions have their own errors?
    FunctionCallInExpressionDidNotReturnSingleValue {
        function_name: String,
    },
    ParseError {
        source: typeql::common::error::Error,
    },
    LiteralParseError {
        literal: String,
        source: LiteralParseError,
    },
    ExpressionDefinition {
        source: ExpressionDefinitionError,
    },
    BuiltinArgumentCountMismatch {
        builtin: String,
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for PatternDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for PatternDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DisjointVariableReuse { .. } => None,
            Self::VariableCategoryMismatch { .. } => None,
            Self::FunctionCallReturnArgCountMismatch { .. } => None,
            Self::FunctionCallArgumentCountMismatch { .. } => None,
            Self::FunctionRequiredArgumentReceivedOptionalVariable { .. } => None,
            Self::UnresolvedFunction { .. } => None,
            Self::ExpectedStreamReceivedSingle { .. } => None,
            Self::ExpectedSingeReceivedStream { .. } => None,
            Self::OptionalVariableForRequiredArgument { .. } => None,
            Self::FunctionCallInExpressionDidNotReturnSingleValue { .. } => None,
            Self::LiteralParseError { .. } => None,
            Self::BuiltinArgumentCountMismatch { .. } => None,
            Self::FunctionRead { source } => Some(source),
            Self::ExpressionDefinition { source } => Some(source),
            Self::ParseError { source } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum LiteralParseError {
    FragmentParseError { fragment: String },
    ScientificNotationNotAllowedForDecimal { literal: String },
    InvalidDate { year: i32, month: u32, day: u32 },
    InvalidTime { hour: u32, minute: u32, second: u32, nano: u32 },
}

impl fmt::Display for LiteralParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for LiteralParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LiteralParseError::FragmentParseError { .. } => None,
            LiteralParseError::ScientificNotationNotAllowedForDecimal { .. } => None,
            LiteralParseError::InvalidDate { .. } => None,
            LiteralParseError::InvalidTime { .. } => None,
        }
    }
}
