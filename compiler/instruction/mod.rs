/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value_type::ValueTypeCategory;

use crate::{
    expression::expression_compiler::ExpressionTreeCompiler,
    inference::ExpressionCompilationError,
    instruction::expression::{
        evaluator::ExpressionEvaluationState, op_codes::ExpressionOpCode, ExpressionEvaluationError,
    },
};

pub mod expression;

pub trait ExpressionInstruction: Sized {
    const OP_CODE: ExpressionOpCode;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError>;
}

pub trait CompilableExpression: ExpressionInstruction {
    fn return_value_category(&self) -> Option<ValueTypeCategory>;

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError>;
}
