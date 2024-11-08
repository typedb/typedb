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
    query::stage::reduce::Reducer,
    statement::{InIterable, StructDeconstruct},
    token,
    value::StringLiteral,
};

use crate::{
    pattern::{expression::ExpressionDefinitionError, variable_category::VariableCategory},
    pipeline::{FunctionReadError, FunctionRepresentationError},
    translation::fetch::FetchRepresentationError,
};

pub mod pattern;
pub mod pipeline;
pub mod translation;

// TODO: include declaration source for each error message
typedb_error!(
    pub RepresentationError(component = "Representation", prefix = "REP") {
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
        FunctionRepresentation(5, "Error translating function into intermediate representation.", ( typedb_source : FunctionRepresentationError )),
        ExpectedStreamFunctionReturnsSingle(
            6,
            "Invalid invocation of function '{function_name}' in an iterable assignment ('in'), since it returns a non-iterable single answer. Use the single-answer assignment '=' instead.",
            function_name: String
        ),
        ExpectedSingleFunctionReturnsStream(
            7,
            "Invalid invocation of function '{function_name}' in a single-answer assignment (=), since it returns an iterable stream of answers. Use the iterable assignment 'in' instead.",
            function_name: String
        ),
        OptionalVariableForRequiredArgument(
            8,
            "Optionally present variable '{input_var}' cannot be used as the non-optional argument '{arg_name}' in function '{name}'.",
            name: String,
            arg_name: String,
            input_var: String
        ),
        InAssignmentMustBeListOrStream(
            9,
            "Iterable assignments ('in') must have an iterable stream or list on the right hand side.\nSource:\n{declaration}",
            declaration: InIterable
        ),
        FunctionReadError(
            10,
            "Error reading function.",
            ( source: FunctionReadError )
        ),
        ParseError(
            11,
            "Error parsing query.",
            ( typedb_source: typeql::Error )
        ),
        LiteralParseError(
            12,
            "Error parsing literal '{literal}'.",
            literal: String,
            ( source: LiteralParseError )
        ),
        ExpressionDefinitionError(
            13,
            "Expression error.",
            ( source: ExpressionDefinitionError )
        ),
        ExpressionAssignmentMustOneVariable(
            14,
            "Expressions must be assigned to a single variable, received {assigned_count} instead.",
            assigned_count: usize
        ),
        ExpressionBuiltinArgumentCountMismatch(
            15,
            "Built-in expression function '{builtin}' expects '{expected}' arguments but received '{actual}' arguments.",
            builtin: token::Function,
            expected: usize,
            actual: usize
        ),
        UnimplementedStructAssignment(
            16,
            "Destructuring structs via assignment is not yet implemented.\nSource:\n{declaration}",
            declaration: StructDeconstruct
        ),
        ScopedRoleNameInRelation(
            17,
            "Relation's declared role types should not contain scopes (':').\nSource:\n{declaration}",
            declaration: typeql::statement::thing::RolePlayer
        ),
        OperatorStageVariableUnavailable(
            18,
            "The variable '{variable_name}' was not available in the stage.\nSource:\n{declaration}",
            variable_name: String,
            declaration: typeql::query::pipeline::stage::Stage
        ),
        ReduceVariableNotAvailable(
            19,
            "The variable '{variable_name}' was not available in for use in the reduce.\nSource:\n{declaration}",
            variable_name: String,
            declaration: Reducer
        ),
        LabelWithKind(
            20,
            "Specifying a kind on a label is not allowed.\nSource:\n{declaration}",
            declaration: typeql::statement::Type
        ),
        LabelWithLabel(
            30,
            "Specifying a label constraint on a label is not allowed.\nSource:\n{declaration}",
            declaration: typeql::Label
        ),
        ScopedLabelWithLabel(
            31,
            "Specifying a scoped label constraint on a label is not allowed.\nSource:\n{declaration}",
            declaration: typeql::ScopedLabel
        ),
        UnrecognisedClause(
            22,
            "Clause type not recognised.\nSource:\n{declaration}",
            declaration: typeql::query::stage::Stage
        ),
        FetchRepresentation(
            23,
            "Error building representation of fetch clause.",
            ( typedb_source : FetchRepresentationError )
        ),
        NonTerminalFetch(
            24,
            "Fetch clauses must be the final clause in a query pipeline.\nSource:\n{declaration}",
            declaration: typeql::query::stage::Stage
        ),
        UnboundVariable(
            25,
            "Invalid query containing unbound concept variable {variable}",
            variable: String
        ),
        ScopedValueTypeName(26, "Value type names cannot have scopes. Provided illegal name: '{scope}:{name}'.", scope: String, name: String),
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
