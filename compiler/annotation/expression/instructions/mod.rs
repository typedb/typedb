/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use encoding::value::value_type::ValueTypeCategory;
use error::typedb_error;

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

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>>;
}

pub(crate) fn check_operation<T>(
    checked_operation_result: Option<T>,
    description: &'static str,
) -> Result<T, ExpressionEvaluationError> {
    match checked_operation_result {
        None => Err(ExpressionEvaluationError::CheckedOperationFailed { description }),
        Some(result) => Ok(result),
    }
}

typedb_error! {
    pub ExpressionEvaluationError(component = "Expression evaluation", prefix = "EEV") {
        ConceptRead(1, "Concept read failed", typedb_source: Box<ConceptReadError>),
        CheckedOperationFailed(2, "Checked operation failed: {description}", description: &'static str),
        CastFailed(3, "Cast failed due to {description}.", description: String),
        DivisionFailed(4, "Division failed, dividend: {dividend}, divisor: {divisor}.", dividend: f64, divisor: f64),
        ListIndexNegative(5, "List index is negative: {index}", index: i64),
        ListIndexOutOfRange(6, "List index out of range {index}, list length: {length}", index: i64, length: usize),
        ListRangeOutOfRange(7, "List range out of range {from_index}..{to_index}, list length: {length}", from_index: i64, to_index: i64, length: usize),
    }
}
