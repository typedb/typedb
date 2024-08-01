/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use encoding::value::{value::DBValue, value_type::ValueTypeCategory, ValueEncodable};

use crate::{
    expressions::{
        evaluator::{ExpressionEvaluationState, ExpressionValue},
        expression_compiler::{ExpressionInstruction, ExpressionTreeCompiler, SelfCompiling},
        op_codes::ExpressionOpCode,
        ExpressionEvaluationError,
    },
    ExpressionCompilationError,
};

// Declarations
pub struct LoadVariable {}
pub struct LoadConstant {}

pub type CastUnaryLongToDouble = CastUnary<i64, f64>;
pub type CastLeftLongToDouble = CastBinaryLeft<i64, f64>;
pub type CastRightLongToDouble = CastBinaryRight<i64, f64>;

// Impls

// Load
impl ExpressionInstruction for LoadVariable {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadVariable;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        match state.next_variable() {
            ExpressionValue::Single(single) => state.push_value(single),
            ExpressionValue::List(list) => state.push_list(list),
        }
        Ok(())
    }
}

impl ExpressionInstruction for LoadConstant {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadConstant;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let constant = state.next_constant();
        state.push_value(constant);
        Ok(())
    }
}

// Casts
pub trait ImplicitCast<From: DBValue>: DBValue {
    const CAST_UNARY_OPCODE: ExpressionOpCode;
    const CAST_LEFT_OPCODE: ExpressionOpCode;
    const CAST_RIGHT_OPCODE: ExpressionOpCode;
    fn cast(from: From) -> Result<Self, ExpressionEvaluationError>;
}

pub struct CastUnary<From: DBValue, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryLeft<From: DBValue, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryRight<From: DBValue, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionInstruction for CastUnary<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_UNARY_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let value_before = From::form_db_value(state.pop_value()).unwrap();
        let value_after = To::cast(value_before)?.to_db_value();
        state.push_value(value_after);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> SelfCompiling for CastUnary<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let value_before = builder.pop_type_single()?;
        if value_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompilationError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryLeft<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_LEFT_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right = state.pop_value();
        let left_before = From::form_db_value(state.pop_value()).unwrap();
        let left_after = To::cast(left_before)?.to_db_value();
        state.push_value(left_after);
        state.push_value(right);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> SelfCompiling for CastBinaryLeft<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let right = builder.pop_type_single()?;
        let left_before = builder.pop_type_single()?;
        if left_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompilationError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY);
        builder.push_type_single(right);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryRight<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_RIGHT_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::form_db_value(state.pop_value()).unwrap();
        let right_after = To::cast(right_before)?.to_db_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> SelfCompiling for CastBinaryRight<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let right_before = builder.pop_type_single()?;
        if right_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompilationError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl ImplicitCast<i64> for f64 {
    const CAST_UNARY_OPCODE: ExpressionOpCode = ExpressionOpCode::CastUnaryLongToDouble;
    const CAST_LEFT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastLeftLongToDouble;
    const CAST_RIGHT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastRightLongToDouble;

    fn cast(from: i64) -> Result<Self, ExpressionEvaluationError> {
        Ok(from as f64)
    }
}
