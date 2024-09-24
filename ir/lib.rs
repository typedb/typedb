/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]
#![allow(unused_variables)]
#![allow(clippy::result_large_err)]

use std::{error::Error, fmt};

use error::typedb_error;
use typeql::{
    statement::{InIterable, StructDeconstruct},
    token,
    value::StringLiteral,
};

use crate::{
    pattern::{expression::ExpressionDefinitionError, variable_category::VariableCategory},
    program::FunctionReadError,
};

pub mod pattern;
pub mod program;
pub mod translation;

// TODO: include declaration source for each error message
typedb_error!(
    pub PatternDefinitionError(component = "Pattern representation", prefix = "PRP") {
        DisjointVariableReuse(
            0,
            "Variable '{name}' is re-used across different branches of the query. Variables that do not represent the same concept must be named uniquely, to prevent clashes within answers.",
            name: String
        ),
        VariableCategoryMismatch(
            1,
            "The variable '{variable_name}' cannot be declared as both a '{category_1}' and as a '{category_2}'.",
            variable_name: String,
            category_1: VariableCategory,
            // category_1_source: Constraint<Variable>,
            category_2: VariableCategory
            // category_2_source: Constraint<Variable>,
        ),
        FunctionCallReturnCountMismatch(
            2,
            "Invalid call to function '{name}', which returns '{function_return_count}' outputs but only '{assigned_var_count}' were assigned.",
            name: String,
            assigned_var_count: usize,
            function_return_count: usize
        ),
        FunctionCallArgumentCountMismatch(
            3,
            "Invalid call to function '{name}', which requires '{expected}' arguments but only '{actual}' were provided.",
            name: String,
            expected: usize,
            actual: usize
        ),
        UnresolvedFunction(4, "Could not resolve function with name '{function_name}'.", function_name: String),
        ExpectedStreamFunctionReturnsSingle(
            5,
            "Invalid invocation of function '{function_name}' in an iterable assignment ('in'), since it returns a non-iterable single answer. Use the single-answer assignment '=' instead.",
            function_name: String
        ),
        ExpectedSingleFunctionReturnsStream(
            6,
            "Invalid invocation of function '{function_name}' in a single-answer assignment (=), since it returns an iterable stream of answers. Use the iterable assignment 'in' instead.",
            function_name: String
        ),
        OptionalVariableForRequiredArgument(
            7,
            "Optionally present variable '{input_var}' cannot be used as the non-optional argument '{arg_name}' in function '{name}'.",
            name: String,
            arg_name: String,
            input_var: String
        ),
        InAssignmentMustBeListOrStream(
            8,
            "Iterable assignments ('in') must have an iterable stream or list on the right hand side.\nSource:\n{declaration}",
            declaration: InIterable
        ),
        FunctionReadError(
            9,
            "Error reading function.",
            ( source: FunctionReadError )
        ),
        ParseError(
            10,
            "Error parsing query.",
            ( typedb_source: typeql::Error )
        ),
        LiteralParseError(
            11,
            "Error parsing literal '{literal}'.",
            literal: String,
            ( source: LiteralParseError )
        ),
        ExpressionDefinitionError(
            12,
            "Expression error.",
            ( source: ExpressionDefinitionError )
        ),
        ExpressionAssignmentMustOneVariable(
            13,
            "Expressions must be assigned to a single variable, received {assigned_count} instead.",
            assigned_count: usize
        ),
        ExpressionBuiltinArgumentCountMismatch(
            14,
            "Built-in expression function '{builtin}' expects '{expected}' arguments but received '{actual}' arguments.",
            builtin: token::Function,
            expected: usize,
            actual: usize
        ),
        UnimplementedStructAssignment(
            15,
            "Destructuring structs via assignment is not yet implemented.\nSource:\n{declaration}",
            declaration: StructDeconstruct
        ),
        ScopedRoleNameInRelation(
            16,
            "Relation's declared role types should not contain scopes (':').\nSource:\n{declaration}",
            declaration: typeql::statement::thing::RolePlayer
        ),
        OperatorStageVariableUnavailable(
            17,
            "The variable '{variable_name}' was not available in the stage.\nSource:\n{declaration}",
            variable_name: String,
            declaration: typeql::query::pipeline::stage::Stage
        ),
        LabelWithKind(
            18,
            "Specifying a kind on a label is not allowed.\nSource:\n{declaration}",
            declaration: typeql::statement::Type
        ),
        LabelWithLabel(
            19,
            "Specifying a label constraint on a label is not allowed.\nSource:\n{declaration}",
            declaration: typeql::Label
        ),
    }
);

#[derive(Debug, Clone)]
pub enum LiteralParseError {
    FragmentParseError { fragment: String },
    ScientificNotationNotAllowedForDecimal { literal: String },
    InvalidDate { year: i32, month: u32, day: u32 },
    InvalidTime { hour: u32, minute: u32, second: u32, nano: u32 },
    CannotUnescapeString { literal: StringLiteral, source: typeql::Error },
    TimeZoneLookup { name: String },
    FixedOffset { value: String },
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
            LiteralParseError::CannotUnescapeString { source, .. } => Some(source),
            LiteralParseError::TimeZoneLookup { .. } => None,
            LiteralParseError::FixedOffset { .. } => None,
        }
    }
}
