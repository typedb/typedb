/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use typeql::common::Span;

use encoding::value::value_type::ValueTypeCategory;
use error::typedb_error;
use ir::{
    pattern::{
        expression::{BuiltInFunctionID, Operator},
        variable_category::VariableCategory,
    },
    RepresentationError,
};

use crate::annotation::expression::instructions::op_codes::ExpressionOpCode;

pub mod block_compiler;
pub mod compiled_expression;
pub mod expression_compiler;
pub mod instructions;

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
        MultipleAssignmentsForVariable(
            7,
            "Variable '{variable}' cannot be assigned to multiple times.",
            variable: String,
            source_span: Option<Span>,
        ),
        CircularDependency(
            8,
            "The variable '{variable}' has an illegal circular expression assignment & usage.",
            variable: String,
            source_span: Option<Span>,
        ),
        CouldNotDetermineValueTypeForVariable(
            9,
            "Could not determine a value type for variable '{variable}'.", 
            variable: String,
            source_span: Option<Span>,
        ),
        VariableMultipleValueTypes(
            10,
            "The variable '{variable}' must have a single possible value type to be used in an expression, but it could have any of: {value_types}.",
            variable: String,
            value_types: String,
            source_span: Option<Span>,
        ),
        VariableMustBeValueOrAttribute(
            11,
            "Variable '{variable}' used in expressions must contain either a value or an attribute, but it is a '{category}'.",
            variable: String,
            category: VariableCategory,
            source_span: Option<Span>,
        ),
        UnsupportedArgumentsForBuiltin(
            12,
            "Built-in function '{function}' cannot be applied to arguments of type '{category}'.",
            function: BuiltInFunctionID,
            category: ValueTypeCategory,
            source_span: Option<Span>,
        ),
        ListIndexMustBeInteger(
            13,
            "List indices must be of integer type.",
            source_span: Option<Span>,
        ),
        HeterogeneusListConstructor(
            14,
            "Values in a constructed list must have the same types.",
            source_span: Option<Span>,
        ),
        EmptyListConstructorCannotInferValueType(
            17,
            "Cannot infer inner value types of an empty list constructor.",
            source_span: Option<Span>,
        ),
        Representation(18, "Error building expression reprentation.", typedb_source: Box<RepresentationError>),
    }
}
