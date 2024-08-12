/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use encoding::value::{value::DBValue, value_type::ValueTypeCategory};

use crate::{
    expression::{expression_compiler::ExpressionCompilationContext, ExpressionCompileError},
    instruction::expression::{
        op_codes::ExpressionOpCode, CompilableExpression, ExpressionEvaluationError, ExpressionInstruction,
    },
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
}

impl ExpressionInstruction for LoadConstant {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadConstant;
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
}

impl<From: DBValue, To: ImplicitCast<From>> CompilableExpression for CastUnary<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let value_before = builder.pop_type_single()?;
        if value_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryLeft<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_LEFT_OPCODE;
}

impl<From: DBValue, To: ImplicitCast<From>> CompilableExpression for CastBinaryLeft<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let right = builder.pop_type_single()?;
        let left_before = builder.pop_type_single()?;
        if left_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(To::VALUE_TYPE_CATEGORY);
        builder.push_type_single(right);

        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionInstruction for CastBinaryRight<From, To> {
    const OP_CODE: ExpressionOpCode = To::CAST_RIGHT_OPCODE;
}

impl<From: DBValue, To: ImplicitCast<From>> CompilableExpression for CastBinaryRight<From, To> {
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(To::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let right_before = builder.pop_type_single()?;
        if right_before != From::VALUE_TYPE_CATEGORY {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
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
