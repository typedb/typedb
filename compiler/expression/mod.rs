/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use answer::variable::Variable;
use concept::error::ConceptReadError;
use encoding::value::value_type::ValueTypeCategory;
use ir::pattern::expression::Operator;
use ir::pattern::variable_category::VariableCategory;

pub mod block_compiler;
pub mod compiled_expression;
pub mod expression_compiler;


#[derive(Debug)]
pub enum ExpressionCompileError {
    ConceptRead { source: ConceptReadError },
    InternalStackWasEmpty,
    InternalUnexpectedValueType,
    UnsupportedOperandsForOperation {
        op: Operator,
        left_category: ValueTypeCategory,
        right_category: ValueTypeCategory,
    },
    MultipleAssignmentsForSingleVariable { assign_variable: Variable },
    CircularDependencyInExpressions { assign_variable: Variable },
    CouldNotDetermineValueTypeForVariable { variable: Variable },
    VariableDidNotHaveSingleValueType { variable: Variable },
    VariableHasNoValueType { variable: Variable },
    VariableMustBeValueOrAttribute { variable: Variable, actual_category: VariableCategory },
    UnsupportedArgumentsForBuiltin,
    ListIndexMustBeLong,
    HeterogenousValuesInList,
    ExpectedSingleWasList,
    ExpectedListWasSingle,
    EmptyListConstructorCannotInferValueType,
}

impl Display for ExpressionCompileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for ExpressionCompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead { source, ..} => Some(source),
            Self::InternalStackWasEmpty
            | Self::InternalUnexpectedValueType
            | Self::UnsupportedOperandsForOperation { .. }
            | Self::MultipleAssignmentsForSingleVariable { .. }
            | Self::CircularDependencyInExpressions { .. }
            | Self::CouldNotDetermineValueTypeForVariable { .. }
            | Self::VariableDidNotHaveSingleValueType { .. }
            | Self::VariableHasNoValueType { .. }
            | Self::VariableMustBeValueOrAttribute { .. }
            | Self::UnsupportedArgumentsForBuiltin
            | Self::ListIndexMustBeLong
            | Self::HeterogenousValuesInList
            | Self::ExpectedSingleWasList
            | Self::ExpectedListWasSingle
            | Self::EmptyListConstructorCannotInferValueType => None,
        }
    }
}
