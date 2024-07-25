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

mod builtins;
pub mod evaluator;
pub mod expression_compiler;
pub mod op_codes;
mod todo__dissolve__builtins;

pub enum ExpressionEvaluationError {
    CheckedOperationFailed,
    CastFailed,
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
}

impl Debug for ExpressionCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for ExpressionCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for ExpressionCompilationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InternalStackWasEmpty => None,
            ExpressionCompilationError::InternalUnexpectedValueType => None,
            ExpressionCompilationError::UnsupportedOperandsForOperation { .. } => None,
        }
    }
}
