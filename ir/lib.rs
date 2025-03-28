/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]
#![allow(unused_variables)]

use error::typedb_error;
use typeql::{common::Span, statement::InIterable, token, value::StringLiteral};

use crate::{
    pattern::{expression::ExpressionRepresentationError, variable_category::VariableCategory},
    pipeline::{FunctionReadError, FunctionRepresentationError},
    translation::fetch::FetchRepresentationError,
};

pub mod pattern;
pub mod pipeline;
pub mod translation;

typedb_error! {
    pub RepresentationError(component = "Representation", prefix = "REP") {
        DisjointVariableReuse(
            0,
            "Variable '{name}' is re-used across different branches of the query. Variables that do not represent the same concept must be named uniquely, to prevent clashes within answers.",
            name: String,
            source_span: Option<Span>,
        ),
        VariableCategoryMismatch(
            1,
            "The variable '{variable_name}' cannot be declared as both a '{category_1}' and as a '{category_2}'.",
            variable_name: String,
            category_1: VariableCategory,
            // category_1_source: Constraint<Variable>,
            category_2: VariableCategory,
            // category_2_source: Constraint<Variable>,
            // TODO: technically, there are 2 source constraint spans here? Can't handle this yet...
        ),
        FunctionCallReturnCountMismatch(
            2,
            "Invalid call to function '{name}', which returns '{function_return_count}' outputs but only '{assigned_var_count}' were assigned.",
            name: String,
            assigned_var_count: usize,
            function_return_count: usize,
            source_span: Option<Span>,
        ),
        FunctionCallArgumentCountMismatch(
            3,
            "Invalid call to function '{name}', which requires '{expected}' arguments but only '{actual}' were provided.",
            name: String,
            expected: usize,
            actual: usize,
            source_span: Option<Span>,
        ),
        UnresolvedFunction(
            4,
            "Could not resolve function with name '{function_name}'.",
            function_name: String,
            source_span: Option<Span>,
        ),
        FunctionRepresentation(
            5,
            "Error translating function into intermediate representation.",
            typedb_source: FunctionRepresentationError
        ),
        ExpectedStreamFunctionReturnsSingle(
            6,
            "Invalid invocation of function '{function_name}' in an iterable assignment ('in'), since it returns a non-iterable single answer. Use the single-answer assignment '=' instead.",
            function_name: String,
            source_span: Option<Span>,
        ),
        ExpectedSingleFunctionReturnsStream(
            7,
            "Invalid invocation of function '{function_name}' in a single-answer assignment (=), since it returns an iterable stream of answers. Use the iterable assignment 'in' instead.",
            function_name: String,
            source_span: Option<Span>,
        ),
        OptionalVariableForRequiredArgument(
            8,
            "Optionally present variable '{input_var}' cannot be used as the non-optional argument '{arg_name}' in function '{name}'.",
            name: String,
            arg_name: String,
            input_var: String,
            source_span: Option<Span>,
        ),
        InAssignmentMustBeListOrStream(
            9,
            "Iterable assignments ('in') must have an iterable stream or list on the right hand side.",
            declaration: InIterable,
            source_span: Option<Span>,
        ),
        FunctionReadError(
            10,
            "Error reading function.",
            typedb_source: FunctionReadError,
        ),
        ParseError(
            11,
            "Error parsing query.",
            typedb_source: typeql::Error,
        ),
        LiteralParseError(
            12,
            "Error parsing literal '{literal}'.",
            literal: String,
            source_span: Option<Span>,
            typedb_source: LiteralParseError,
        ),
        ExpressionRepresentationError(
            13,
            "Expression error.",
            typedb_source: ExpressionRepresentationError,
            source_span: Option<Span>,
        ),
        ExpressionAssignmentMustOneVariable(
            14,
            "Expressions must be assigned to a single variable, received {assigned_count} instead.",
            assigned_count: usize,
            source_span: Option<Span>,
        ),
        ExpressionBuiltinArgumentCountMismatch(
            15,
            "Built-in expression function '{builtin}' expects '{expected}' arguments but received '{actual}' arguments.",
            builtin: token::Function,
            expected: usize,
            actual: usize,
            source_span: Option<Span>,
        ),
        UnimplementedStructAssignment(
            16,
            "Destructuring structs via assignment is not yet implemented.",
            source_span: Option<Span>,
        ),
        ScopedRoleNameInRelation(
            17,
            "Relation's declared role types should not contain scopes (':').",
            source_span: Option<Span>,
        ),
        OperatorStageVariableUnavailable(
            18,
            "The variable '{variable_name}' was not available in the stage.",
            variable_name: String,
            source_span: Option<Span>,
        ),
        ReduceVariableNotAvailable(
            19,
            "The variable '{variable_name}' was not available in for use in the reduce.",
            variable_name: String,
            source_span: Option<Span>,
        ),
        LabelWithKind(
            20,
            "Specifying a kind on a label is not allowed.",
            source_span: Option<Span>,
        ),
        AssigningToInputVariable(
            21,
            "The variable '{variable}' may not be assigned to, as it was already bound in a previous stage",
            variable: String,
            source_span: Option<Span>,
        ),
        LabelWithLabel(
            30,
            "Specifying a label constraint on a label is not allowed.",
            source_span: Option<Span>,
        ),
        ScopedLabelWithLabel(
            31,
            "Specifying a scoped label constraint on a label is not allowed.",
            source_span: Option<Span>,
        ),
        UnrecognisedClause(
            22,
            "Clause type not recognised.",
            source_span: Option<Span>,
        ),
        FetchRepresentation(
            23,
            "Error building representation of fetch stage.",
            typedb_source: Box<FetchRepresentationError>,
        ),
        NonTerminalFetch(
            24,
            "Fetch clauses must be the final clause in a query pipeline.",
            source_span: Option<Span>,
        ),
        UnboundVariable(
            25,
            "Invalid query containing unbound concept variable {variable}.",
            variable: String,
            source_span: Option<Span>,
        ),
        ReservedKeywordAsIdentifier(
            27,
            "A reserved keyword '{identifier}' was used as identifier.",
            identifier: typeql::Identifier,
            source_span: Option<Span>,
        ),
        VariableCategoryMismatchInIs(
            28,
            "The variable categories for the is statement are incompatible.",
            lhs_variable: String,
            rhs_variable: String,
            lhs_category: VariableCategory,
            rhs_category: VariableCategory,
            source_span: Option<Span>,
        ),
        UpdateVariableUnavailable(
            39,
            "The variable '{variable}' referenced in the update stage is unavailable. It should be bound in the previous stage.",
            variable: String,
            source_span: Option<Span>,
        ),
        DeleteVariableUnavailable(
            40,
            "The variable '{variable}' referenced in the delete stage is unavailable. It should be bound in the previous stage.",
            variable: String,
            source_span: Option<Span>,
        ),
        NonAnonymousVariableExpected(
            41,
            "A non-anonymous variable is expected in this statement for the query.",
            source_span: Option<Span>,
        ),
        IllegalStatementForUpdate(
            42,
            "Illegal statement provided for an update stage. Only 'update $0 has $1' and 'update $0 links $1' are allowed.",
            source_span: Option<Span>,
        ),
        IllegalStatementForPut(
            43,
            "Illegal statement '{constraint_type}' provided for a put stage. Only 'has', 'links' and 'isa' constraints are allowed.",
            constraint_type: String,
            source_span: Option<Span>,
        ),
        UnboundRequiredVariable(
            44,
            "{variable} is required to be bound to a value before it's used.",
            variable: String,
            source_span: Option<Span>,
            _rest: Vec<Option<Span>>,
        ),
        RegexExpectedStringLiteral(
            50,
            "Expected a string literal as regex.",
            source_span: Option<Span>,
        ),
        RegexFailedCompilation(
            51,
            "The regular expression failed compilation: '{value}'.",
            value: String,
            source: regex::Error,
            source_span: Option<Span>,
        ),
        UnimplementedLanguageFeature(
            254,
            "The language feature is not yet implemented: {feature}.",
            feature: error::UnimplementedFeature,
        ),
        UnimplementedOptionalType(
            255,
            "Optional types are not yet implemented.",
            source_span: Option<Span>,
            feature: error::UnimplementedFeature
        ),
        UnimplementedListType(
            256,
            "List types are not yet implemented.",
            source_span: Option<Span>,
            feature: error::UnimplementedFeature
        ),
    }
}

typedb_error! {
    pub LiteralParseError(component = "Literal parse", prefix = "LIT") {
        FragmentParseError(
            1,
            "Failed to parse literal fragment into primitive: {fragment}.",
            fragment: String,
            source_span: Option<Span>,
        ),
        ScientificNotationNotAllowedForDecimal(
            2,
            "Fixed-point decimal values cannot use scientific notation: {literal}.",
            literal: String,
            source_span: Option<Span>,
        ),
        InvalidDate(
            3,
            "Invalid date with year {year}, month {month}, day {day}",
            year: i32,
            month: u32,
            day: u32,
            source_span: Option<Span>,
        ),
        InvalidTime(
            4,
            "Invalid time with hour {hour}, minute {minute}, second {second}, nanoseconds {nano}",
            hour: u32,
            minute: u32,
            second: u32,
            nano: u32,
            source_span: Option<Span>,
        ),
        CannotUnescapeString(
            5,
            "Cannot unescape string literal: '{literal}'.",
            literal: StringLiteral,
            source_span: Option<Span>,
            typedb_source: typeql::Error,
        ),
        CannotUnescapeRegexString(
            6,
            "Cannot unescape regex string: '{literal}'.",
            literal: StringLiteral,
            source_span: Option<Span>,
            typedb_source: typeql::Error
        ),
        InvalidTimezoneNamed(
            7,
            "Unrecognised timezone '{name}'.",
            name: String,
            source_span: Option<Span>,
        ),
        InvalidTimezoneFixedOffset(
            8,
            "Invalid timezone offset '{offset}'.",
            offset: String,
            source_span: Option<Span>,
        ),
        UnimplementedLanguageFeature(9, "Unimplemented '{feature}'.", feature: error::UnimplementedFeature),
    }
}
