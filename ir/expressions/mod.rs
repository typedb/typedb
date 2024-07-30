/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use encoding::value::value_type::ValueTypeCategory;

use crate::pattern::expression::Operator;

pub(crate) mod builtins;
pub mod evaluator;
pub mod expression_compiler;
pub mod op_codes;

pub enum ExpressionEvaluationError {
    CheckedOperationFailed,
    CastFailed,
    ListIndexOutOfRange,
}

impl Debug for ExpressionEvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for ExpressionEvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for ExpressionEvaluationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckedOperationFailed => None,
            Self::CastFailed => None,
            Self::ListIndexOutOfRange => None,
        }
    }
}

pub enum ExpressionCompilationError {
    InternalStackWasEmpty,
    InternalUnexpectedValueType,
    UnsupportedOperandsForOperation {
        op: Operator,
        left_category: ValueTypeCategory,
        right_category: ValueTypeCategory,
    },
    UnsupportedArgumentsForBuiltin,
    ListIndexMustBeLong,
    HeterogenousValuesInList,
    ExpectedSingleWasList,
    ExpectedListWasSingle,
    EmptyListConstructorCannotInferValueType,
}

impl Debug for ExpressionCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO
        match self {
            ExpressionCompilationError::InternalStackWasEmpty => f.write_str("ExpressionCompilationError::InternalStackWasEmpty"),
            ExpressionCompilationError::InternalUnexpectedValueType => f.write_str("ExpressionCompilationError::InternalUnexpectedValueType"),
            ExpressionCompilationError::UnsupportedOperandsForOperation { .. } => f.write_str("ExpressionCompilationError::UnsupportedOperandsForOperation"),
            ExpressionCompilationError::UnsupportedArgumentsForBuiltin => f.write_str("ExpressionCompilationError::UnsupportedArgumentsForBuiltin"),
            ExpressionCompilationError::ListIndexMustBeLong => f.write_str("ExpressionCompilationError::ListIndexMustBeLong"),
            ExpressionCompilationError::HeterogenousValuesInList => f.write_str("ExpressionCompilationError::HeterogenousValuesInList"),
            ExpressionCompilationError::ExpectedSingleWasList => f.write_str("ExpressionCompilationError::ExpectedSingleWasList"),
            ExpressionCompilationError::ExpectedListWasSingle => f.write_str("ExpressionCompilationError::ExpectedListWasSingle"),
            ExpressionCompilationError::EmptyListConstructorCannotInferValueType => f.write_str("ExpressionCompilationError::EmptyListConstructorCannotInferValueType"),
        }
    }
}

impl Display for ExpressionCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO
        match self {
            ExpressionCompilationError::InternalStackWasEmpty => f.write_str("ExpressionCompilationError::InternalStackWasEmpty"),
            ExpressionCompilationError::InternalUnexpectedValueType => f.write_str("ExpressionCompilationError::InternalUnexpectedValueType"),
            ExpressionCompilationError::UnsupportedOperandsForOperation { .. } => f.write_str("ExpressionCompilationError::UnsupportedOperandsForOperation"),
            ExpressionCompilationError::UnsupportedArgumentsForBuiltin => f.write_str("ExpressionCompilationError::UnsupportedArgumentsForBuiltin"),
            ExpressionCompilationError::ListIndexMustBeLong => f.write_str("ExpressionCompilationError::ListIndexMustBeLong"),
            ExpressionCompilationError::HeterogenousValuesInList => f.write_str("ExpressionCompilationError::HeterogenousValuesInList"),
            ExpressionCompilationError::ExpectedSingleWasList => f.write_str("ExpressionCompilationError::ExpectedSingleWasList"),
            ExpressionCompilationError::ExpectedListWasSingle => f.write_str("ExpressionCompilationError::ExpectedListWasSingle"),
            ExpressionCompilationError::EmptyListConstructorCannotInferValueType => f.write_str("ExpressionCompilationError::EmptyListConstructorCannotInferValueType"),
        }
    }
}

impl Error for ExpressionCompilationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InternalStackWasEmpty => None,
            Self::InternalUnexpectedValueType => None,
            Self::UnsupportedOperandsForOperation { .. } => None,
            Self::UnsupportedArgumentsForBuiltin => None,
            Self::ListIndexMustBeLong => None,
            Self::HeterogenousValuesInList => None,
            Self::ExpectedSingleWasList => None,
            Self::ExpectedListWasSingle => None,
            Self::EmptyListConstructorCannotInferValueType => None,
        }
    }
}
