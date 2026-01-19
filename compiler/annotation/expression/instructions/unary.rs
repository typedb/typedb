/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, marker::PhantomData};

use encoding::value::{decimal_value::Decimal, value::NativeValueConvertible, value_type::ValueTypeCategory};

use crate::annotation::expression::{
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        op_codes::ExpressionOpCode, CompilableExpression, ExpressionEvaluationError, ExpressionInstruction,
    },
    ExpressionCompileError,
};

pub trait UnaryExpression<'a, T1: NativeValueConvertible<'a>, R: NativeValueConvertible<'a>> {
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: T1) -> Result<R, ExpressionEvaluationError>;
}

pub struct Unary<'a, T1, R, F>
where
    T1: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: UnaryExpression<'a, T1, R>,
{
    phantom: PhantomData<&'a (T1, R, F)>,
}

impl<'a, T1, R, F> ExpressionInstruction for Unary<'a, T1, R, F>
where
    T1: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: UnaryExpression<'a, T1, R>,
{
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
}

impl<'a, T1, R, F> CompilableExpression for Unary<'a, T1, R, F>
where
    T1: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: UnaryExpression<'a, T1, R>,
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(R::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let a1 = builder.pop_type_single()?.category();
        if a1 != T1::VALUE_TYPE_CATEGORY {
            Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: F::OP_CODE,
                expected: T1::VALUE_TYPE_CATEGORY,
                actual: a1,
            }))?;
        }
        builder.push_type_single(R::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! unary_instruction {
    ( $lt:lifetime $( $name:ident = $impl_name:ident($a1:ident: $t1:ty) -> $r:ty $impl_code:block )* ) => { $(
        pub type $name<'a> = Unary<'a, $t1, $r, $impl_name>;
        pub struct $impl_name {}
        impl<'a> UnaryExpression<'a, $t1, $r> for $impl_name {
            const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
            fn evaluate($a1: $t1) -> Result<$r, ExpressionEvaluationError> {
                $impl_code
            }
        })*
    };
}

pub(crate) use unary_instruction;

unary_instruction! { 'a
    MathAbsInteger = MathAbsIntegerImpl(a1: i64) -> i64 { Ok(i64::abs(a1)) }
    MathAbsDouble = MathAbsDoubleImpl(a1: f64) -> f64 { Ok(f64::abs(a1)) }
    MathAbsDecimal = MathAbsDecimalImpl(a1: Decimal) -> Decimal { Ok(Decimal::abs(a1)) }

    MathRoundDouble = MathRoundDoubleImpl(a1: f64) -> i64 { Ok(f64::round_ties_even(a1) as i64) }
    MathCeilDouble = MathCeilDoubleImpl(a1: f64) -> i64 { Ok(f64::ceil(a1) as i64) }
    MathFloorDouble = MathFloorDoubleImpl(a1: f64) -> i64 { Ok(f64::floor(a1) as i64) }

    MathRoundDecimal = MathRoundDecimalImpl(a1: Decimal) -> i64 { Ok(Decimal::round(a1)) }
    MathCeilDecimal = MathCeilDecimalImpl(a1: Decimal) -> i64 { Ok(Decimal::ceil(a1)) }
    MathFloorDecimal = MathFloorDecimalImpl(a1: Decimal) -> i64 { Ok(Decimal::floor(a1)) }

    LengthString = LengthStringImpl(a1: Cow<'a, str>) -> i64 {
        let len = a1.len();
        len.try_into().map_err(|_| ExpressionEvaluationError::OverlongString { len })
    }
}
