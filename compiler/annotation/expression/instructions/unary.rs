/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, marker::PhantomData};

use encoding::value::{decimal_value::Decimal, value::NativeValueConvertible, value_type::ValueTypeCategory};

use crate::annotation::expression::{
    ExpressionCompileError,
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        CompilableExpression, ExpressionEvaluationError, ExpressionInstruction, op_codes::ExpressionOpCode,
    },
};

pub trait UnaryExpression<'a> {
    type T1: NativeValueConvertible<'a>;
    type R: NativeValueConvertible<'a>;
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: Self::T1) -> Result<Self::R, ExpressionEvaluationError>;
}

pub struct Unary<'a, F: UnaryExpression<'a>>(PhantomData<&'a F>);

impl<'a, F: UnaryExpression<'a>> ExpressionInstruction for Unary<'a, F> {
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
}

impl<'a, F: UnaryExpression<'a>> CompilableExpression for Unary<'a, F> {
    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let a1 = builder.pop_type_single()?.category();
        if a1 != F::T1::VALUE_TYPE_CATEGORY {
            Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: F::OP_CODE,
                expected: F::T1::VALUE_TYPE_CATEGORY,
                actual: a1,
            }))?;
        }
        builder.push_type_single(F::R::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! unary_instruction {
    ( $lt:lifetime $( $name:ident($a1:ident: $t1:ty) -> $r:ty $impl_code:block )* ) => {
        paste::paste!{
            $(
            pub type $name<$lt> = Unary<$lt, [<$name Impl>]>;
            pub struct [<$name Impl>] {}
            impl<$lt> UnaryExpression<$lt> for [<$name Impl>] {
                const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
                type T1 = $t1;
                type R = $r;
                fn evaluate($a1: $t1) -> Result<$r, ExpressionEvaluationError> {
                    $impl_code
                }
            })*
        }
    };
}

unary_instruction! { 'a
    MathAbsInteger(a1: i64) -> i64 { Ok(i64::abs(a1)) }
    MathAbsDouble(a1: f64) -> f64 { Ok(f64::abs(a1)) }
    MathAbsDecimal(a1: Decimal) -> Decimal { Ok(Decimal::abs(a1)) }

    MathRoundDouble(a1: f64) -> i64 { Ok(f64::round_ties_even(a1) as i64) }
    MathCeilDouble(a1: f64) -> i64 { Ok(f64::ceil(a1) as i64) }
    MathFloorDouble(a1: f64) -> i64 { Ok(f64::floor(a1) as i64) }

    MathRoundDecimal(a1: Decimal) -> i64 { Ok(Decimal::round(a1)) }
    MathCeilDecimal(a1: Decimal) -> i64 { Ok(Decimal::ceil(a1)) }
    MathFloorDecimal(a1: Decimal) -> i64 { Ok(Decimal::floor(a1)) }

    LenString(a1: Cow<'a, str>) -> i64 {
        let len = a1.chars().count();
        len.try_into().map_err(|_| ExpressionEvaluationError::OverlongString { len })
    }
}
