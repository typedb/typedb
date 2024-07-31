/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use encoding::value::value::DBValue;
use encoding::value::value_type::ValueTypeCategory;

use crate::{
    expression::expression_compiler::ExpressionCompilationContext,
    inference::ExpressionCompilationError,
    instruction::expression::op_codes::ExpressionOpCode,
};
use crate::instruction::expression::builtins::binary::BinaryExpression;
use crate::instruction::expression::builtins::load_cast::ImplicitCast;
use crate::instruction::expression::builtins::unary::UnaryExpression;

pub mod builtins;
pub mod op_codes;

pub trait ExpressionInstruction: Sized {
    const OP_CODE: ExpressionOpCode;
}

pub trait CompilableExpression: ExpressionInstruction {
    fn return_value_category(&self) -> Option<ValueTypeCategory>;

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompilationError>;
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
