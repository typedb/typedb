/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use encoding::value::value_type::ValueTypeCategory;

use crate::expressions::{
    evaluator::ExpressionEvaluationState,
    expression_compiler::{ExpressionInstruction, ExpressionTreeCompiler, SelfCompiling},
    op_codes::ExpressionOpCode,
    todo__dissolve__builtins::ValueTypeTrait,
    ExpressionCompilationError, ExpressionEvaluationError,
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
        let var = state.next_variable();
        state.push_value(var);
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
pub trait ImplicitCast<From: ValueTypeTrait>: ValueTypeTrait {
    const CAST_UNARY_OPCODE: ExpressionOpCode;
    const CAST_LEFT_OPCODE: ExpressionOpCode;
    const CAST_RIGHT_OPCODE: ExpressionOpCode;
    fn cast(from: From) -> Result<Self, ExpressionEvaluationError>;
}

pub struct CastUnary<From: ValueTypeTrait, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryLeft<From: ValueTypeTrait, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryRight<From: ValueTypeTrait, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> ExpressionInstruction for CastUnary<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_UNARY_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::from_value(state.pop_value()).map_err(|_| ExpressionEvaluationError::CastFailed)?;
        let right_after = To::cast(right_before)?.into_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> SelfCompiling for CastUnary<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let _right_before = builder.pop_mock()?;
        builder.push_mock(To::mock_value());

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryLeft<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_LEFT_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right = state.pop_value();
        let left_before = From::from_value(state.pop_value()).map_err(|_| ExpressionEvaluationError::CastFailed)?;
        let left_after = To::cast(left_before)?.into_value();
        state.push_value(left_after);
        state.push_value(right);
        Ok(())
    }
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> SelfCompiling for CastBinaryLeft<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let right = builder.pop_mock()?;
        let _left_before = builder.pop_mock()?;
        builder.push_mock(To::mock_value());
        builder.push_mock(right);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryRight<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_RIGHT_OPCODE;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::from_value(state.pop_value()).map_err(|_| ExpressionEvaluationError::CastFailed)?;
        let right_after = To::cast(right_before)?.into_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<From: ValueTypeTrait, To: ImplicitCast<From>> SelfCompiling for CastBinaryRight<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let _right_before = builder.pop_mock()?;
        builder.push_mock(To::mock_value());

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
