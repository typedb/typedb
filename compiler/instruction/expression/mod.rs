/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use binary::BinaryExpression;
use encoding::value::{value::DBValue, value_type::ValueTypeCategory};
use load_cast::ImplicitCast;
use unary::UnaryExpression;

use crate::{
    expression::{expression_compiler::ExpressionCompilationContext, ExpressionCompileError},
    instruction::expression::op_codes::ExpressionOpCode,
};

pub mod binary;
pub mod list_operations;
pub mod load_cast;
pub mod op_codes;
pub mod operators;
pub mod unary;

pub trait ExpressionInstruction: Sized {
    const OP_CODE: ExpressionOpCode;
}

pub trait CompilableExpression: ExpressionInstruction {
    fn return_value_category(&self) -> Option<ValueTypeCategory>;

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError>;
}

pub(crate) fn check_operation<T>(checked_operation_result: Option<T>) -> Result<T, ExpressionEvaluationError> {
    match checked_operation_result {
        None => Err(ExpressionEvaluationError::CheckedOperationFailed),
        Some(result) => Ok(result),
    }
}

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
