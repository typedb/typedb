/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::marker::PhantomData;

use encoding::value::{decimal_value::Decimal, value::NativeValueConvertible, value_type::ValueTypeCategory};

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

pub type CastUnaryIntegerToDouble<'a> = CastUnary<'a, i64, f64>;
pub type CastLeftIntegerToDouble<'a> = CastBinaryLeft<'a, i64, f64>;
pub type CastRightIntegerToDouble<'a> = CastBinaryRight<'a, i64, f64>;

pub type CastUnaryDecimalToDouble<'a> = CastUnary<'a, Decimal, f64>;
pub type CastLeftDecimalToDouble<'a> = CastBinaryLeft<'a, Decimal, f64>;
pub type CastRightDecimalToDouble<'a> = CastBinaryRight<'a, Decimal, f64>;

pub type CastUnaryIntegerToDecimal<'a> = CastUnary<'a, i64, Decimal>;
pub type CastLeftIntegerToDecimal<'a> = CastBinaryLeft<'a, i64, Decimal>;
pub type CastRightIntegerToDecimal<'a> = CastBinaryRight<'a, i64, Decimal>;

// Impls

// Load
impl ExpressionInstruction for LoadVariable {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadVariable;
}

impl ExpressionInstruction for LoadConstant {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadConstant;
}

// Casts
pub trait ImplicitCast<'a, From: NativeValueConvertible<'a>>: NativeValueConvertible<'a> {
    const CAST_UNARY_OPCODE: ExpressionOpCode;
    const CAST_LEFT_OPCODE: ExpressionOpCode;
    const CAST_RIGHT_OPCODE: ExpressionOpCode;
    fn cast(from: From) -> Result<Self, ExpressionEvaluationError>;
}

pub struct CastUnary<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> {
    phantom: PhantomData<&'a (From, To)>,
}

pub struct CastBinaryLeft<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> {
    phantom: PhantomData<&'a (From, To)>,
}

pub struct CastBinaryRight<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> {
    phantom: PhantomData<&'a (From, To)>,
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionInstruction
    for CastUnary<'a, From, To>
{
    const OP_CODE: ExpressionOpCode = To::CAST_UNARY_OPCODE;
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> CompilableExpression
    for CastUnary<'a, From, To>
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let value_before = builder.pop_type_single()?.category();
        if value_before != From::VALUE_TYPE_CATEGORY {
            return Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: Self::OP_CODE,
                expected: From::VALUE_TYPE_CATEGORY,
                actual: value_before,
            }));
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionInstruction
    for CastBinaryLeft<'a, From, To>
{
    const OP_CODE: ExpressionOpCode = To::CAST_LEFT_OPCODE;
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> CompilableExpression
    for CastBinaryLeft<'a, From, To>
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let right = builder.pop_type_single()?;
        let left_before = builder.pop_type_single()?.category();
        if left_before != From::VALUE_TYPE_CATEGORY {
            Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: Self::OP_CODE,
                expected: From::VALUE_TYPE_CATEGORY,
                actual: left_before,
            }))?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.push_type_single(right);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionInstruction
    for CastBinaryRight<'a, From, To>
{
    const OP_CODE: ExpressionOpCode = To::CAST_RIGHT_OPCODE;
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> CompilableExpression
    for CastBinaryRight<'a, From, To>
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let right_before = builder.pop_type_single()?.category();
        if right_before != From::VALUE_TYPE_CATEGORY {
            Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: Self::OP_CODE,
                expected: From::VALUE_TYPE_CATEGORY,
                actual: right_before,
            }))?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl ImplicitCast<'_, i64> for f64 {
    const CAST_UNARY_OPCODE: ExpressionOpCode = ExpressionOpCode::CastUnaryIntegerToDouble;
    const CAST_LEFT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastLeftIntegerToDouble;
    const CAST_RIGHT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastRightIntegerToDouble;

    fn cast(from: i64) -> Result<Self, ExpressionEvaluationError> {
        Ok(from as f64)
    }
}

impl ImplicitCast<'_, Decimal> for f64 {
    const CAST_UNARY_OPCODE: ExpressionOpCode = ExpressionOpCode::CastUnaryDecimalToDouble;
    const CAST_LEFT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastLeftDecimalToDouble;
    const CAST_RIGHT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastRightDecimalToDouble;

    fn cast(from: Decimal) -> Result<Self, ExpressionEvaluationError> {
        Ok(from.to_f64())
    }
}

impl ImplicitCast<'_, i64> for Decimal {
    const CAST_UNARY_OPCODE: ExpressionOpCode = ExpressionOpCode::CastUnaryIntegerToDecimal;
    const CAST_LEFT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastLeftIntegerToDecimal;
    const CAST_RIGHT_OPCODE: ExpressionOpCode = ExpressionOpCode::CastRightIntegerToDecimal;

    fn cast(from: i64) -> Result<Self, ExpressionEvaluationError> {
        Ok(Decimal::new(from, 0))
    }
}
