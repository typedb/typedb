/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value_type::ValueTypeCategory;
use error::typedb_error;
use ir::{
    RepresentationError,
    pattern::expression::{BuiltinValueFunctionID, Operator},
};
use typeql::common::Span;

use crate::annotation::expression::instructions::op_codes::ExpressionOpCode;

mod builtin_resolution;
pub mod compiled_expression;
pub mod expression_compiler;
pub mod instructions;
mod operation_resolution;

typedb_error! {
    pub ExpressionCompileError(component = "Expression compilation", prefix = "CEX") {
        ExpressionMismatchedValueType(
            1,
            "Unexpected internal error - expected value type '{expected}', found '{actual}' for compiled operator '{op_code}'.",
            op_code: ExpressionOpCode,
            expected: ValueTypeCategory,
            actual: ValueTypeCategory,
        ),
        InternalStackWasEmpty(2, "Unexpected internal error - expression execution stack was empty."),
        InternalExpectedSingleWasList(3, "Unexpected internal error - expression compilation expected a singular element and received a list."),
        InternalExpectedListWasSingle(4, "Unexpected internal error - expression compilation expected a list but received a singular element"),
        InternalListLengthMustBeInteger(5, "Unexpected internal error - computed list length constant must be an integer"),
        UnsupportedOperandsForOperation(
            6,
            "Expression operation '{op}' cannot be used with left type '{left_category}' and right type '{right_category}'.",
            op: Operator,
            left_category: ValueTypeCategory,
            right_category: ValueTypeCategory,
            source_span: Option<Span>,
        ),
        UnsupportedArgumentsForBuiltin(
            12,
            "Built-in function '{function}' cannot be applied to arguments of type '{category}'.",
            function: BuiltinValueFunctionID,
            category: ValueTypeCategory,
            source_span: Option<Span>,
        ),
        UnsupportedDifferentArgumentForBuiltin(
            13,
            "Built-in function '{function}' expects matching argument types, but received one argument of type '{arg_1_category}' but another of type '{arg_2_category}'.",
            function: BuiltinValueFunctionID,
            arg_1_category: ValueTypeCategory,
            arg_2_category: ValueTypeCategory,
            source_span: Option<Span>,
        ),
        ListIndexMustBeInteger(
            14,
            "List indices must be of integer type.",
            source_span: Option<Span>,
        ),
        HeterogeneusListConstructor(
            15,
            "Values in a constructed list must have the same types.",
            source_span: Option<Span>,
        ),
        EmptyListConstructorCannotInferValueType(
            17,
            "Cannot infer inner value types of an empty list constructor.",
            source_span: Option<Span>,
        ),
        ReassigningValueVariableFromPreviousStage(
            18,
            "The variable '{variable}' cannot be assigned to, as it was already assigned in a previous stage.",
            variable: String,
        ),
        ValueVariableConflictingAssignmentTypes(
            19,
            "All assignments of the variable '{variable}' must have the same value type. Found: {value_types}.",
            variable: String,
            value_types: String,
            source_span: Option<Span>,
        ),
        Representation(20, "Error building expression representation.", typedb_source: Box<RepresentationError>),
    }
}
