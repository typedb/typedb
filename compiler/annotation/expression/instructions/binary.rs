/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{cmp, marker::PhantomData, ops::Rem};

use encoding::value::{decimal_value::Decimal, value::NativeValueConvertible};

use crate::annotation::expression::{
    ExpressionCompileError,
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        CompilableExpression, ExpressionEvaluationError, ExpressionInstruction, op_codes::ExpressionOpCode,
    },
};

pub trait BinaryExpression<'a> {
    type T1: NativeValueConvertible<'a>;
    type T2: NativeValueConvertible<'a>;
    type R: NativeValueConvertible<'a>;
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: Self::T1, a2: Self::T2) -> Result<Self::R, ExpressionEvaluationError>;
}

pub struct Binary<'a, E: BinaryExpression<'a>>(PhantomData<&'a E>);

impl<'a, E: BinaryExpression<'a>> ExpressionInstruction for Binary<'a, E> {
    const OP_CODE: ExpressionOpCode = E::OP_CODE;
}

impl<'a, E: BinaryExpression<'a>> CompilableExpression for Binary<'a, E> {
    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let a2 = builder.pop_type_single()?.category();
        let a1 = builder.pop_type_single()?.category();
        if a1 != E::T1::VALUE_TYPE_CATEGORY {
            return Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: E::OP_CODE,
                expected: E::T1::VALUE_TYPE_CATEGORY,
                actual: a1,
            }));
        }
        if a2 != E::T2::VALUE_TYPE_CATEGORY {
            return Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: E::OP_CODE,
                expected: E::T2::VALUE_TYPE_CATEGORY,
                actual: a2,
            }));
        }
        builder.push_type_single(E::R::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! binary_instruction {
    ( $lt:lifetime $( $name:ident($a1:ident: $t1:ty, $a2:ident: $t2:ty) -> $r:ty $impl_code:block )* ) => {
        paste::paste!{
            $(
            pub type $name<$lt> = Binary<$lt, [<$name Impl>]>;
            pub struct [<$name Impl>] {}
            impl<$lt> BinaryExpression<$lt> for [<$name Impl>] {
                const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
                type T1 = $t1;
                type T2 = $t2;
                type R = $r;
                fn evaluate($a1: $t1, $a2: $t2) -> Result<$r, ExpressionEvaluationError> {
                    $impl_code
                }
            })*
        }
    };
}

pub(crate) use binary_instruction;

binary_instruction! { 'a
    MathRemainderInteger(a1: i64, a2: i64) -> i64 { Ok(i64::rem(a1, a2)) }

    MathMinIntegerInteger(a1: i64, a2: i64) -> i64 { Ok(cmp::min(a1, a2)) }
    MathMinDoubleDouble(a1: f64, a2: f64) -> f64 { Ok(f64::min(a1, a2)) }
    MathMinDecimalDecimal(a1: Decimal, a2: Decimal) -> Decimal { Ok(cmp::min(a1, a2)) }

    MathMaxIntegerInteger(a1: i64, a2: i64) -> i64 { Ok(cmp::max(a1, a2)) }
    MathMaxDoubleDouble(a1: f64, a2: f64) -> f64 { Ok(f64::max(a1, a2)) }
    MathMaxDecimalDecimal(a1: Decimal, a2: Decimal) -> Decimal { Ok(cmp::max(a1, a2)) }
}
