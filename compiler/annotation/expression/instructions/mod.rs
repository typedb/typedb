/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use encoding::value::value_type::ValueTypeCategory;

use crate::annotation::expression::{
    expression_compiler::ExpressionCompilationContext, instructions::op_codes::ExpressionOpCode, ExpressionCompileError,
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

#[derive(Clone)]
pub enum ExpressionEvaluationError {
    CheckedOperationFailed,
    CastFailed,
    ListIndexOutOfRange,
}

impl fmt::Debug for ExpressionEvaluationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl fmt::Display for ExpressionEvaluationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
