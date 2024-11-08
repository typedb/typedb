/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::marker::PhantomData;

use encoding::value::{value::NativeValueConvertible, value_type::ValueTypeCategory};
use encoding::value::decimal_value::Decimal;
use crate::annotation::expression::{
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        op_codes::ExpressionOpCode, CompilableExpression, ExpressionEvaluationError, ExpressionInstruction,
    },
    ExpressionCompileError,
};

// Declarations
pub struct LoadVariable {}
pub struct LoadConstant {}

pub type CastUnaryLongToDouble = CastUnary<i64, f64>;
pub type CastLeftLongToDouble = CastBinaryLeft<i64, f64>;
pub type CastRightLongToDouble = CastBinaryRight<i64, f64>;

pub type CastUnaryDecimalToDouble = CastUnary<Decimal, f64>;
pub type CastLeftDecimalToDouble = CastBinaryLeft<Decimal, f64>;
pub type CastRightDecimalToDouble = CastBinaryRight<Decimal, f64>;

// Impls

// Load
impl ExpressionInstruction for LoadVariable {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadVariable;
}

impl ExpressionInstruction for LoadConstant {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadConstant;
}

// Casts
pub trait ImplicitCast<From: NativeValueConvertible>: NativeValueConvertible {
    const CAST_UNARY_OPCODE: ExpressionOpCode;
    const CAST_LEFT_OPCODE: ExpressionOpCode;
    const CAST_RIGHT_OPCODE: ExpressionOpCode;
    fn cast(from: From) -> Result<Self, ExpressionEvaluationError>;
}

pub struct CastUnary<From: NativeValueConvertible, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryLeft<From: NativeValueConvertible, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

pub struct CastBinaryRight<From: NativeValueConvertible, To: ImplicitCast<From>> {
    phantom: PhantomData<(From, To)>,
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionInstruction for CastUnary<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_UNARY_OPCODE;
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> CompilableExpression for CastUnary<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let value_before = builder.pop_type_single()?.category();
        if value_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryLeft<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_LEFT_OPCODE;
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> CompilableExpression for CastBinaryLeft<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let right = builder.pop_type_single()?;
        let left_before = builder.pop_type_single()?.category();
        if left_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.push_type_single(right);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryRight<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_RIGHT_OPCODE;
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> CompilableExpression for CastBinaryRight<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        dbg!(&builder.type_stack);
        let right_before = builder.pop_type_single()?.category();
        if right_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());

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

impl ImplicitCast<Decimal> for f64 {
    const CAST_UNARY_OPCODE: ExpressionOpCode = ExpressionOpCode::CastUnaryDecimalToDouble;
    const CAST_LEFT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastLeftDecimalToDouble;
    const CAST_RIGHT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastRightDecimalToDouble;

    fn cast(from: Decimal) -> Result<Self, ExpressionEvaluationError> {
        Ok(from.to_f64())
    }
}
